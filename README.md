# AlphaFlowing FIX Demo (Customer Reference)

This project is a QuickFIX/J initiator demo for validating AlphaFlowing FIX gateway behavior.

## Delivery Model (Public + Private)

- Public package:
  - source code
  - `customer.properties.template`
- Private package (distributed out-of-band):
  - runtime properties file (recommended name: `customer.private.properties`)
  - optional secret material (for example truststore/password)

The demo runs in **Strict External** mode:

- `fixdemo.configFile` is required.
- Runtime defaults are not embedded in code.

## Prerequisites

- Java 21+
- Maven 3.9+
- PowerShell 7+ (for helper script)

## 5-Minute Quickstart

1. Create a private runtime config from the template:

```powershell
Copy-Item customer.properties.template customer.private.properties
```

2. Fill all required keys in `customer.private.properties`.

3. Provide password securely via environment variable (recommended):

```powershell
$env:FIXDEMO_PASSWORD_PLAIN = "<your-plain-password>"
```

4. Run smoke suite:

```powershell
.\scripts\run-conformance.ps1 -ConfigFile customer.private.properties -Suite smoke
```

## Full Conformance Run

```powershell
.\scripts\run-conformance.ps1 -ConfigFile customer.private.properties -Suite all
```

Or directly via Maven:

```powershell
mvn -q `
  -Dfixdemo.configFile=customer.private.properties `
  exec:java
```

Optional temporary override via `-Dfixdemo.*`:

```powershell
mvn -q `
  -Dfixdemo.configFile=customer.private.properties `
  -Dfixdemo.suite=smoke `
  exec:java
```

## Required Runtime Keys

`fixdemo.host`, `fixdemo.port`, `fixdemo.targetCompId`, `fixdemo.clientId`, `fixdemo.symbol`, `fixdemo.dictionary.path`, `fixdemo.tls.enabledProtocols`, `fixdemo.suite`, `fixdemo.report.out`, `fixdemo.onlyLogon`.

Password:

- Recommended: `FIXDEMO_PASSWORD_PLAIN` environment variable.
- Compatibility fallback: `fixdemo.passwordPlain` in private properties (not recommended).

## Configuration Priority

1. `FIXDEMO_PASSWORD_PLAIN` (password only)
2. JVM system properties (`-Dfixdemo.*`)
3. Config file from `-Dfixdemo.configFile` (or script `-ConfigFile`)

No code-level runtime defaults are used.

## Output Files

Each run creates:

`target/conformance/run-YYYYMMDD-HHMMSS/`

- `conformance-report.md` - human-readable test report
- `conformance-report.json` - machine-readable test report
- `sanitized-session.log` - session log with sensitive tags masked (`553`, `554`)

## Security Notes

- Do not commit private runtime properties.
- Do not pass plain password in CLI args unless absolutely necessary.
- `-Dfixdemo.passwordPlain` remains supported for compatibility but is not recommended.

## Troubleshooting Matrix

| Symptom | Typical Cause | What to Check |
| --- | --- | --- |
| `logon timeout` | Session rejected or disconnected by server | Verify `49/553` alignment (`clientId`), `56` target, account enablement, IP allow-list, server-side logs |
| `invalid symbol` / `E_EXT_INPUT_INVALID_FIX_SYMBOL_INVALID` | Unsupported or disabled symbol | Verify configured `fixdemo.symbol` with server |
| `49!=553` or sender/username mismatch | Identity policy violation | Ensure `fixdemo.clientId` is used consistently as sender and username |
| `56 mismatch` / invalid target | Wrong target CompID | Verify `fixdemo.targetCompId` exactly matches server config |
| `dictionary missing` | Wrong FIX dictionary path | Set `fixdemo.dictionary.path` to an existing dictionary file |
| TLS handshake / SSL issues | Trust or protocol mismatch | Confirm public CA chain, TLS 1.2/1.3 support, optional truststore settings |
