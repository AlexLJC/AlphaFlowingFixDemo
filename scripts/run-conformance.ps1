param(
    [Parameter(Mandatory = $false)]
    [string]$FixHost,

    [Parameter(Mandatory = $false)]
    [int]$Port,

    [Parameter(Mandatory = $false)]
    [string]$TargetCompId,

    [Parameter(Mandatory = $false)]
    [string]$ClientId,

    [Parameter(Mandatory = $false)]
    [string]$PasswordPlain,

    [Parameter(Mandatory = $false)]
    [string]$Symbol,

    [Parameter(Mandatory = $false)]
    [ValidateSet("all", "smoke", "negative")]
    [string]$Suite,

    [Parameter(Mandatory = $false)]
    [string]$ReportOut,

    [Parameter(Mandatory = $false)]
    [string]$DictionaryPath,

    [Parameter(Mandatory = $false)]
    [string]$TrustStore,

    [Parameter(Mandatory = $false)]
    [string]$TrustStorePassword,

    [Parameter(Mandatory = $false)]
    [string]$EnabledProtocols,

    [Parameter(Mandatory = $false)]
    [string]$ConfigFile = "customer.private.properties"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Add-Prop {
    param(
        [Parameter(Mandatory = $true)][string]$Key,
        [Parameter(Mandatory = $false)][object]$Value
    )
    if ($null -eq $Value) {
        return
    }
    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) {
        return
    }
    $script:props += "-D$Key=$text"
}

function Config-HasPassword {
    param([string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path) -or -not (Test-Path -LiteralPath $Path)) {
        return $false
    }
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line -match '^\s*fixdemo\.passwordPlain\s*=\s*(.+)$') {
            return -not [string]::IsNullOrWhiteSpace($Matches[1])
        }
    }
    return $false
}

function Read-ConfigValue {
    param(
        [string]$Path,
        [string]$Key
    )
    if ([string]::IsNullOrWhiteSpace($Path) -or -not (Test-Path -LiteralPath $Path)) {
        return $null
    }
    $escaped = [regex]::Escape($Key)
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line -match "^\s*$escaped\s*=\s*(.+)$") {
            return $Matches[1].Trim()
        }
    }
    return $null
}

if ([string]::IsNullOrWhiteSpace($ConfigFile)) {
    throw "ConfigFile is required (for Strict External mode)."
}
$configPath = (Resolve-Path -LiteralPath $ConfigFile -ErrorAction SilentlyContinue)
if ($null -eq $configPath) {
    throw "Config file not found: $ConfigFile"
}
$configPath = $configPath.Path
$configHasPassword = Config-HasPassword -Path $configPath

$props = @()
Add-Prop -Key "fixdemo.configFile" -Value $configPath

if ($PSBoundParameters.ContainsKey("FixHost")) {
    Add-Prop -Key "fixdemo.host" -Value $FixHost
}
if ($PSBoundParameters.ContainsKey("Port")) {
    Add-Prop -Key "fixdemo.port" -Value $Port
}
if ($PSBoundParameters.ContainsKey("TargetCompId")) {
    Add-Prop -Key "fixdemo.targetCompId" -Value $TargetCompId
}
if ($PSBoundParameters.ContainsKey("ClientId")) {
    Add-Prop -Key "fixdemo.clientId" -Value $ClientId
}
if ($PSBoundParameters.ContainsKey("Symbol")) {
    Add-Prop -Key "fixdemo.symbol" -Value $Symbol
}
if ($PSBoundParameters.ContainsKey("Suite")) {
    Add-Prop -Key "fixdemo.suite" -Value $Suite
}
if ($PSBoundParameters.ContainsKey("ReportOut")) {
    Add-Prop -Key "fixdemo.report.out" -Value $ReportOut
}
if ($PSBoundParameters.ContainsKey("DictionaryPath")) {
    Add-Prop -Key "fixdemo.dictionary.path" -Value $DictionaryPath
}
if ($PSBoundParameters.ContainsKey("TrustStore")) {
    Add-Prop -Key "fixdemo.tls.trustStore" -Value $TrustStore
}
if ($PSBoundParameters.ContainsKey("TrustStorePassword")) {
    Add-Prop -Key "fixdemo.tls.trustStorePassword" -Value $TrustStorePassword
}
if ($PSBoundParameters.ContainsKey("EnabledProtocols")) {
    Add-Prop -Key "fixdemo.tls.enabledProtocols" -Value $EnabledProtocols
}

$effectivePassword = $env:FIXDEMO_PASSWORD_PLAIN
if ([string]::IsNullOrWhiteSpace($effectivePassword) -and -not [string]::IsNullOrWhiteSpace($PasswordPlain)) {
    $effectivePassword = $PasswordPlain
}

if ([string]::IsNullOrWhiteSpace($effectivePassword) -and -not $configHasPassword) {
    $secure = Read-Host "Plain password (injected via FIXDEMO_PASSWORD_PLAIN for this run)" -AsSecureString
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    try {
        $effectivePassword = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

$previousPassword = $env:FIXDEMO_PASSWORD_PLAIN
$hadPreviousPassword = $null -ne (Get-ChildItem Env:FIXDEMO_PASSWORD_PLAIN -ErrorAction SilentlyContinue)
if (-not [string]::IsNullOrWhiteSpace($effectivePassword)) {
    $env:FIXDEMO_PASSWORD_PLAIN = $effectivePassword
}

try {
    Write-Host "Running FIX conformance suite..." -ForegroundColor Cyan
    Write-Host "ConfigFile=$configPath"
    $displayTarget = if ($PSBoundParameters.ContainsKey("TargetCompId")) {
        $TargetCompId
    } else {
        Read-ConfigValue -Path $configPath -Key "fixdemo.targetCompId"
    }
    $displayClient = if ($PSBoundParameters.ContainsKey("ClientId")) {
        $ClientId
    } else {
        Read-ConfigValue -Path $configPath -Key "fixdemo.clientId"
    }
    $displaySuite = if ($PSBoundParameters.ContainsKey("Suite")) {
        $Suite
    } else {
        Read-ConfigValue -Path $configPath -Key "fixdemo.suite"
    }
    $displaySymbol = if ($PSBoundParameters.ContainsKey("Symbol")) {
        $Symbol
    } else {
        Read-ConfigValue -Path $configPath -Key "fixdemo.symbol"
    }
    Write-Host "Target=$displayTarget Client=$displayClient Suite=$displaySuite Symbol=$displaySymbol"

    mvn -q @props exec:java

    if ($LASTEXITCODE -ne 0) {
        throw "Conformance run failed with exit code $LASTEXITCODE"
    }
} finally {
    if ($hadPreviousPassword) {
        $env:FIXDEMO_PASSWORD_PLAIN = $previousPassword
    } else {
        Remove-Item Env:FIXDEMO_PASSWORD_PLAIN -ErrorAction SilentlyContinue
    }
}
