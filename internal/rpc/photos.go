package rpc

import (
	"context"
	"errors"

	"github.com/gotd/td/tg"

	"telesrv/internal/domain"
)

// registerPhotos 注册 photos.* RPC handler（头像上传 / 切换 / 查询 / 删除）。
func (r *Router) registerPhotos(d *tg.ServerDispatcher) {
	d.OnPhotosUploadProfilePhoto(r.onPhotosUploadProfilePhoto)
	d.OnPhotosUpdateProfilePhoto(r.onPhotosUpdateProfilePhoto)
	d.OnPhotosGetUserPhotos(r.onPhotosGetUserPhotos)
	d.OnPhotosDeletePhotos(r.onPhotosDeletePhotos)
}

func (r *Router) onPhotosUploadProfilePhoto(ctx context.Context, req *tg.PhotosUploadProfilePhotoRequest) (*tg.PhotosPhoto, error) {
	if r.deps.Files == nil {
		return nil, notImplementedErr()
	}
	userID, ok, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if !ok || userID == 0 {
		return nil, photoInvalidErr()
	}
	file, hasFile := req.GetFile()
	if !hasFile {
		// 仅 fallback / video / emoji markup 等本阶段不支持的头像变体。
		return nil, photoInvalidErr()
	}
	ref, ok := uploadedFileRef(userID, file)
	if !ok {
		return nil, fileReferenceInvalidErr()
	}
	photo, err := r.deps.Files.UploadProfilePhoto(ctx, domain.PeerTypeUser, userID, ref, int(r.clock.Now().Unix()))
	if err != nil {
		return nil, photoUploadErr(err)
	}
	return r.photosPhotoForSelf(ctx, userID, photo), nil
}

func (r *Router) onPhotosUpdateProfilePhoto(ctx context.Context, req *tg.PhotosUpdateProfilePhotoRequest) (*tg.PhotosPhoto, error) {
	if r.deps.Files == nil {
		return nil, notImplementedErr()
	}
	userID, ok, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if !ok || userID == 0 {
		return nil, photoInvalidErr()
	}
	switch in := req.ID.(type) {
	case *tg.InputPhoto:
		photo, found, err := r.deps.Files.SetCurrentProfilePhoto(ctx, domain.PeerTypeUser, userID, in.ID, int(r.clock.Now().Unix()))
		if err != nil {
			return nil, internalErr()
		}
		if !found {
			return nil, photoInvalidErr()
		}
		return r.photosPhotoForSelf(ctx, userID, photo), nil
	default:
		// InputPhotoEmpty：移除当前头像（停用现有当前照片）。
		if cur, found, err := r.deps.Files.CurrentProfilePhoto(ctx, domain.PeerTypeUser, userID); err == nil && found {
			_, _ = r.deps.Files.DeleteProfilePhotos(ctx, domain.PeerTypeUser, userID, []int64{cur.ID})
		}
		return r.photosPhotoForSelf(ctx, userID, domain.Photo{}), nil
	}
}

func (r *Router) onPhotosGetUserPhotos(ctx context.Context, req *tg.PhotosGetUserPhotosRequest) (tg.PhotosPhotosClass, error) {
	if r.deps.Files == nil {
		return &tg.PhotosPhotos{}, nil
	}
	currentUserID, ok, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if !ok || currentUserID == 0 {
		return nil, userIDInvalidErr()
	}
	target, found, err := r.userFromInput(ctx, currentUserID, req.UserID)
	if err != nil {
		return nil, internalErr()
	}
	if !found {
		return nil, userIDInvalidErr()
	}
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	limit := req.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	photos, total, err := r.deps.Files.GetProfilePhotos(ctx, domain.PeerTypeUser, target.ID, offset, limit, req.MaxID)
	if err != nil {
		return nil, internalErr()
	}
	tgPhotos := make([]tg.PhotoClass, 0, len(photos))
	for _, p := range photos {
		tgPhotos = append(tgPhotos, tgPhoto(p))
	}
	users := []tg.UserClass{r.tgUser(target)}
	if total > len(photos)+offset {
		return &tg.PhotosPhotosSlice{Count: total, Photos: tgPhotos, Users: users}, nil
	}
	return &tg.PhotosPhotos{Photos: tgPhotos, Users: users}, nil
}

func (r *Router) onPhotosDeletePhotos(ctx context.Context, id []tg.InputPhotoClass) ([]int64, error) {
	if r.deps.Files == nil {
		return []int64{}, nil
	}
	userID, ok, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	if !ok || userID == 0 {
		return nil, photoInvalidErr()
	}
	ids := make([]int64, 0, len(id))
	for _, in := range id {
		if photo, isPhoto := in.(*tg.InputPhoto); isPhoto && photo.ID != 0 {
			ids = append(ids, photo.ID)
		}
	}
	if len(ids) == 0 {
		return []int64{}, nil
	}
	if _, err := r.deps.Files.DeleteProfilePhotos(ctx, domain.PeerTypeUser, userID, ids); err != nil {
		return nil, internalErr()
	}
	return ids, nil
}

// photosPhotoForSelf 组装 photos.photo 响应（新照片 + 带头像的 self user），并在头像变更后
// 向该账号其它在线设备推送，使其即时刷新头像。仅由 uploadProfilePhoto / updateProfilePhoto
// 等头像变更路径调用（只读路径不得使用，否则会误触发推送）。
func (r *Router) photosPhotoForSelf(ctx context.Context, userID int64, photo domain.Photo) *tg.PhotosPhoto {
	out := &tg.PhotosPhoto{Photo: tgPhoto(photo), Users: []tg.UserClass{}}
	if r.deps.Users == nil {
		return out
	}
	self, err := r.deps.Users.Self(ctx, userID)
	if err != nil {
		return out
	}
	out.Users = append(out.Users, r.tgSelfUser(self))
	r.pushSelfPhotoUpdate(ctx, self)
	return out
}

// pushSelfPhotoUpdate 向该账号其它在线设备推送头像变更。updateUserName 不含 photo 无法刷新
// 头像；updateUser 只是「该 user 变了」的信号（TDesktop 仅当 peer 已 full-loaded 时才
// forceFull 重拉）；最可靠是在 Updates.Users 带上含新 userProfilePhoto 的完整 self user，
// TDesktop 经 processUser→setPhoto→peerUpdated(Photo) 即时刷新。当前设备同时经 RPC 返回更新，
// 重复推送对 TDesktop 幂等（setUserpicChecked 比对 photo_id，相同则 no-op）。
func (r *Router) pushSelfPhotoUpdate(ctx context.Context, self domain.User) {
	if self.ID == 0 {
		return
	}
	r.pushUserUpdates(ctx, self.ID, &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateUser{UserID: self.ID}},
		Users:   []tg.UserClass{r.tgSelfUser(self)},
		Date:    int(r.clock.Now().Unix()),
	})
}

// uploadedFileRef 把 tg.InputFile / InputFileBig 转成 domain.UploadedFileRef。
func uploadedFileRef(ownerUserID int64, file tg.InputFileClass) (domain.UploadedFileRef, bool) {
	switch f := file.(type) {
	case *tg.InputFile:
		if f.ID == 0 || f.Parts <= 0 {
			return domain.UploadedFileRef{}, false
		}
		return domain.UploadedFileRef{OwnerUserID: ownerUserID, FileID: f.ID, Parts: f.Parts, Name: f.Name, MD5: f.MD5Checksum}, true
	case *tg.InputFileBig:
		if f.ID == 0 || f.Parts <= 0 {
			return domain.UploadedFileRef{}, false
		}
		return domain.UploadedFileRef{OwnerUserID: ownerUserID, FileID: f.ID, Parts: f.Parts, Name: f.Name, Big: true}, true
	default:
		return domain.UploadedFileRef{}, false
	}
}

// resolveInputChatPhoto 把 tg.InputChatPhoto 解析为 *domain.Photo（nil=清除头像）。
// 支持新上传（InputChatUploadedPhoto）与引用已有照片（InputChatPhoto{InputPhoto}）。
func (r *Router) resolveInputChatPhoto(ctx context.Context, userID int64, input tg.InputChatPhotoClass) (*domain.Photo, error) {
	switch in := input.(type) {
	case *tg.InputChatPhotoEmpty:
		return nil, nil
	case *tg.InputChatUploadedPhoto:
		file, ok := in.GetFile()
		if !ok {
			return nil, photoInvalidErr()
		}
		if r.deps.Files == nil {
			return nil, photoInvalidErr()
		}
		ref, ok := uploadedFileRef(userID, file)
		if !ok {
			return nil, fileReferenceInvalidErr()
		}
		// 频道/群头像用 avatar 尺寸（'a'/'c'），匹配 InputPeerPhotoFileLocation 下载路径。
		photo, err := r.deps.Files.CreateAvatarFromUpload(ctx, ref)
		if err != nil {
			return nil, photoUploadErr(err)
		}
		return &photo, nil
	case *tg.InputChatPhoto:
		switch id := in.ID.(type) {
		case *tg.InputPhoto:
			if r.deps.Files == nil {
				return nil, photoInvalidErr()
			}
			photo, found, err := r.deps.Files.GetPhoto(ctx, id.ID)
			if err != nil {
				return nil, internalErr()
			}
			if !found {
				return nil, photoInvalidErr()
			}
			return &photo, nil
		default:
			return nil, nil // InputPhotoEmpty → 清除
		}
	default:
		return nil, photoInvalidErr()
	}
}

func photoUploadErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrFilePartsInvalid):
		return filePartsInvalidErr()
	case errors.Is(err, domain.ErrPhotoInvalid):
		return photoInvalidErr()
	default:
		return internalErr()
	}
}
