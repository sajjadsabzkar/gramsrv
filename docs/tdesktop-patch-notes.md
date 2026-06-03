# Telegram Desktop Patch Notes

These notes describe the minimal Telegram Desktop patch required to connect a
self-built client to `telesrv`.

## Target

| Item | Value |
|---|---|
| Telegram Desktop commit | `9caf32dffc90ddd9bb08ad5777b865f729fa167b` |
| Describe | `v6.8.4-15-g9caf32dffc` |
| TL layer | 225 |
| Local DC id | `2` |
| Local endpoint | `127.0.0.1:2398` |
| IPv6 endpoint | `[::1]:2398` |

`telesrv` defaults match this patch:

```powershell
TELESRV_LISTEN=0.0.0.0:2398
TELESRV_ADVERTISE_IP=127.0.0.1
TELESRV_DC=2
```

## RSA Key

Telegram Desktop must contain the public key that matches the private key used
by your local `telesrv` instance.

Start `telesrv` once so it creates `data/server_rsa.pem`, then export:

```powershell
openssl rsa -in data/server_rsa.pem -RSAPublicKey_out -out data/server_rsa.pub
```

Copy the PEM contents of `data/server_rsa.pub` into both `kPublicRSAKeys` and
`kTestPublicRSAKeys` in:

```text
Telegram/SourceFiles/mtproto/mtproto_dc_options.cpp
```

## DC List Patch

In the same file, replace the built-in DC arrays with:

```cpp
const BuiltInDc kBuiltInDcs[] = {
    { 2, "127.0.0.1", 2398 },
};

const BuiltInDc kBuiltInDcsIPv6[] = {
    { 2, "::1", 2398 },
};

const BuiltInDc kBuiltInDcsTest[] = {
    { 2, "127.0.0.1", 2398 },
};

const BuiltInDc kBuiltInDcsIPv6Test[] = {
    { 2, "::1", 2398 },
};
```

In `DcOptions::constructFromBuiltIn()`, mark built-in endpoints TCP-only:

```cpp
const auto flags = Flag::f_static | Flag::f_tcpo_only;
const auto flags = Flag::f_static | Flag::f_ipv6 | Flag::f_tcpo_only;
```

That is the whole client patch: local DC endpoints, matching RSA public key,
and TCP-only flags.

## Build

Follow Telegram Desktop's upstream build docs for your platform. For Windows
x64 at the pinned baseline:

```powershell
git clone --recursive https://github.com/telegramdesktop/tdesktop.git
cd tdesktop
git checkout 9caf32dffc90ddd9bb08ad5777b865f729fa167b
git submodule update --init --recursive

Telegram\build\prepare\win.bat
cd Telegram
configure.bat x64 -D TDESKTOP_API_ID=YOUR_API_ID -D TDESKTOP_API_HASH=YOUR_API_HASH
```

Open `out\Telegram.slnx` in Visual Studio and build the `Telegram` project.

## Multi-Client Local Test

Run two clients with isolated working directories:

```powershell
$tdesktop = "C:\path\to\tdesktop\out\Debug\Telegram.exe"
Start-Process $tdesktop -ArgumentList @("-workdir", "$PWD\.tdata-alice")
Start-Process $tdesktop -ArgumentList @("-workdir", "$PWD\.tdata-bob")
```

Use different phone numbers for Alice and Bob. The default development login
code is `12345`.

## Verification Checklist

- `telesrv` logs `tl_layer=225` and listens on `2398`.
- Telegram Desktop connects without reconnect loops.
- No new `NOT_IMPLEMENTED`, `Unhandled RPC`, `bad_msg`, panic, or internal error
  appears in server logs during startup and basic messaging.
- A second client with a separate `-workdir` can sign in and exchange messages
  with the first client.

Keep every client-side workaround mapped to a server-side compatibility decision
and keep protocol changes separate from UI experiments.