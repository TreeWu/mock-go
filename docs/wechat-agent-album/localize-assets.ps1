param(
    [string]$ArchiveDir = $PSScriptRoot
)

$ErrorActionPreference = 'Stop'

$indexPath = Join-Path $ArchiveDir 'index.json'
$imageDir = Join-Path $ArchiveDir 'images'
$index = Get-Content $indexPath -Raw | ConvertFrom-Json
$htmlFiles = Get-ChildItem $ArchiveDir -Filter '*.html' -File
$urlPattern = 'https?://(?:mmbiz\.qpic\.cn|res\.wx\.qq\.com)/[^&"'' <>\)]+(?:&amp;[A-Za-z0-9_]+=[^&"'' <>\)]*)*'
$remoteUrls = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)

function Get-AssetKey {
    param([string]$Url)

    $builder = [System.UriBuilder]([uri]$Url)
    $builder.Scheme = 'https'
    $builder.Port = -1
    return $builder.Uri.GetLeftPart([System.UriPartial]::Path)
}

function Write-Utf8TextFile {
    param(
        [string]$Path,
        [string]$Content
    )

    for ($attempt = 1; $attempt -le 5; $attempt++) {
        try {
            [System.IO.File]::WriteAllText($Path, $Content, [System.Text.UTF8Encoding]::new($false))
            return
        }
        catch [System.IO.IOException] {
            if ($attempt -eq 5) { throw }
            [GC]::Collect()
            [GC]::WaitForPendingFinalizers()
            Start-Sleep -Milliseconds (200 * $attempt)
        }
    }
}

foreach ($htmlFile in $htmlFiles) {
    $html = Get-Content $htmlFile.FullName -Raw
    foreach ($match in [regex]::Matches($html, $urlPattern, 'IgnoreCase')) {
        [void]$remoteUrls.Add(($match.Value -replace '&amp;', '&'))
    }
}

$sourcesByPath = @{}
foreach ($remoteUrl in $remoteUrls) {
    $key = Get-AssetKey $remoteUrl
    if (-not $sourcesByPath.ContainsKey($key) -or $remoteUrl.Length -lt $sourcesByPath[$key].Length) {
        $sourcesByPath[$key] = $remoteUrl
    }
}

$localByPath = @{}
foreach ($article in $index.articles) {
    for ($i = 0; $i -lt $article.images.Count; $i++) {
        $key = Get-AssetKey $article.images[$i]
        $localByPath[$key] = $article.localImages[$i]
    }
}

$additionalImages = @(
    $index.additionalImages |
        Group-Object file |
        ForEach-Object { $_.Group[0] }
)
foreach ($additionalImage in $additionalImages) {
    $key = Get-AssetKey $additionalImage.source
    $localByPath[$key] = $additionalImage.file
}

foreach ($entry in $sourcesByPath.GetEnumerator() | Sort-Object Name) {
    if ($localByPath.ContainsKey($entry.Name)) { continue }

    $source = $entry.Value -replace '^http:', 'https:'
    $formatMatch = [regex]::Match($source, '[?&]wx_fmt=([A-Za-z0-9]+)')
    $extension = if ($formatMatch.Success) { $formatMatch.Groups[1].Value.ToLowerInvariant() } else { 'jpg' }
    if ($extension -eq 'jpeg') { $extension = 'jpg' }

    $hashBytes = [System.Security.Cryptography.SHA256]::HashData(
        [System.Text.Encoding]::UTF8.GetBytes($entry.Name)
    )
    $hash = [Convert]::ToHexString($hashBytes).Substring(0, 12).ToLowerInvariant()
    $relativePath = "images/extra-$hash.$extension"
    $targetPath = Join-Path $ArchiveDir $relativePath

    if (-not (Test-Path $targetPath)) {
        Invoke-WebRequest -Uri $source -OutFile $targetPath -Headers @{
            Referer = 'https://mp.weixin.qq.com/'
            'User-Agent' = 'Mozilla/5.0'
        }
    }

    $localByPath[$entry.Name] = $relativePath
    $additionalImages += [pscustomobject]@{
        source = $source
        file = $relativePath
    }
}

$srcRegex = [regex]::new('(?<![-\w])src="[^"]*"', 'IgnoreCase')
$styleRegex = [regex]::new('(?<![-\w])style="([^"]*)"', 'IgnoreCase')
foreach ($htmlFile in $htmlFiles) {
    $html = Get-Content $htmlFile.FullName -Raw
    $html = [regex]::Replace($html, $urlPattern, {
        param($match)

        $decoded = $match.Value -replace '&amp;', '&'
        $key = Get-AssetKey $decoded
        if ($localByPath.ContainsKey($key)) { return $localByPath[$key] }
        return $match.Value
    }, 'IgnoreCase')

    $html = [regex]::Replace($html, '<img\b[^>]*>', {
        param($match)

        $tag = $match.Value
        $dataSrc = [regex]::Match(
            $tag,
            '(?<![-\w])data-src="(images/[^"]+)"',
            'IgnoreCase'
        )
        if (-not $dataSrc.Success) { return $tag }

        $replacement = 'src="' + $dataSrc.Groups[1].Value + '"'
        if ($srcRegex.IsMatch($tag)) {
            return $srcRegex.Replace($tag, $replacement, 1)
        }
        return $tag -replace '>$', (' ' + $replacement + '>')
    }, 'IgnoreCase')

    $html = [regex]::Replace($html, '<[^>]+data-lazy-bgimg="images/[^"]+"[^>]*>', {
        param($match)

        $tag = $match.Value
        $lazyBackground = [regex]::Match(
            $tag,
            '(?<![-\w])data-lazy-bgimg="(images/[^"]+)"',
            'IgnoreCase'
        )
        if (-not $lazyBackground.Success) { return $tag }

        $localBackground = $lazyBackground.Groups[1].Value
        if ($styleRegex.IsMatch($tag)) {
            return $styleRegex.Replace($tag, {
                param($styleMatch)
                $style = [regex]::Replace(
                    $styleMatch.Groups[1].Value,
                    '\s*;?\s*background-image:\s*url\(images/[A-Za-z0-9._-]+\)\s*!important;?',
                    '',
                    'IgnoreCase'
                )
                return 'style="' + $style.TrimEnd() +
                    '; background-image: url(' + $localBackground + ') !important;"'
            }, 1)
        }
        return $tag -replace '>$', (' style="background-image: url(' + $localBackground + ') !important;">')
    }, 'IgnoreCase')

    Write-Utf8TextFile -Path $htmlFile.FullName -Content $html
}

$index | Add-Member -NotePropertyName additionalImages -NotePropertyValue $additionalImages -Force
$index | Add-Member -NotePropertyName additionalImageCount -NotePropertyValue $additionalImages.Count -Force
$index | Add-Member -NotePropertyName offlineImageCount -NotePropertyValue ($index.imageDownloaded + $additionalImages.Count) -Force
$index | Add-Member -NotePropertyName localizedAt -NotePropertyValue ([DateTime]::UtcNow.ToString('o')) -Force
$json = $index | ConvertTo-Json -Depth 12
Write-Utf8TextFile -Path $indexPath -Content ($json + "`n")

[pscustomobject]@{
    HtmlFiles = $htmlFiles.Count
    ArticleImages = $index.imageDownloaded
    AdditionalImages = $additionalImages.Count
    OfflineImages = $index.offlineImageCount
}
