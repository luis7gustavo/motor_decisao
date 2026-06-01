param(
    [string]$InputPath = "C:\Users\luisg\Downloads\Tabela Produtos Geral 26.xls",
    [string]$OutputPath = ""
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path $ProjectRoot "data\coletek_catalog_raw.json"
}

function Convert-ToPrice {
    param([object]$Value)

    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) {
        return $null
    }
    if ($Value -is [double] -or $Value -is [decimal] -or $Value -is [int]) {
        return [double]$Value
    }

    $number = 0.0
    $text = ([string]$Value).Trim()
    $style = [System.Globalization.NumberStyles]::Float
    $invariant = [System.Globalization.CultureInfo]::InvariantCulture
    $ptBr = [System.Globalization.CultureInfo]::GetCultureInfo("pt-BR")
    if ([double]::TryParse($text, $style, $invariant, [ref]$number)) {
        return $number
    }
    if ([double]::TryParse($text, $style, $ptBr, [ref]$number)) {
        return $number
    }
    return $null
}

function Get-CellText {
    param(
        [object]$Values,
        [int]$Row,
        [object]$Column
    )

    if ($null -eq $Column) {
        return ""
    }
    return ([string]$Values[$Row, [int]$Column]).Trim()
}

$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$excel.DisplayAlerts = $false
$excel.AskToUpdateLinks = $false
$productsByReference = @{}
$snapshotDate = (Get-Item -LiteralPath $InputPath).LastWriteTime.ToString("yyyy-MM-dd")

try {
    $workbook = $excel.Workbooks.Open($InputPath, 0, $true)
    foreach ($worksheet in $workbook.Worksheets) {
        $usedRange = $worksheet.UsedRange
        $rowCount = $usedRange.Rows.Count
        $columnCount = [Math]::Min($usedRange.Columns.Count, 12)
        $values = $worksheet.Range(
            $worksheet.Cells.Item(1, 1),
            $worksheet.Cells.Item($rowCount, $columnCount)
        ).Value2

        $headerRow = $null
        for ($row = 1; $row -le [Math]::Min($rowCount, 8); $row++) {
            for ($column = 1; $column -le $columnCount; $column++) {
                if (([string]$values[$row, $column]).Trim() -match "REFER") {
                    $headerRow = $row
                    break
                }
            }
            if ($null -ne $headerRow) {
                break
            }
        }
        if ($null -eq $headerRow) {
            continue
        }

        $columns = @{}
        for ($column = 1; $column -le $columnCount; $column++) {
            $header = ([string]$values[$headerRow, $column]).Trim().ToUpperInvariant()
            if ($header -match "REFER") { $columns.reference = $column }
            elseif ($header -match "DESCRI") { $columns.description = $column }
            elseif ($header -eq "MARCA") { $columns.brand = $column }
            elseif ($header -eq "NCM") { $columns.ncm = $column }
            elseif ($header -eq "TIPO") { $columns.product_type = $column }
            elseif ($header -match "TOT IMP") { $columns.price = $column }
            elseif ($header -match "ESTOQUE") { $columns.stock_text = $column }
        }

        $urlsByRow = @{}
        foreach ($hyperlink in $worksheet.Hyperlinks) {
            if (-not [string]::IsNullOrWhiteSpace([string]$hyperlink.Address)) {
                $urlsByRow[[int]$hyperlink.Range.Row] = [string]$hyperlink.Address
            }
            [System.Runtime.InteropServices.Marshal]::ReleaseComObject($hyperlink) | Out-Null
        }

        for ($row = $headerRow + 1; $row -le $rowCount; $row++) {
            $reference = Get-CellText $values $row $columns.reference
            $description = Get-CellText $values $row $columns.description
            if ([string]::IsNullOrWhiteSpace($reference) -or [string]::IsNullOrWhiteSpace($description)) {
                continue
            }

            $section = $worksheet.Name.Trim()
            $phaseOut = $section -eq "PHASE OUT"
            $candidate = [ordered]@{
                reference = $reference
                description = $description
                brand = Get-CellText $values $row $columns.brand
                ncm = Get-CellText $values $row $columns.ncm
                product_type = Get-CellText $values $row $columns.product_type
                price = Convert-ToPrice $(if ($null -ne $columns.price) { $values[$row, $columns.price] } else { $null })
                stock_text = Get-CellText $values $row $columns.stock_text
                source_url = [string]$urlsByRow[$row]
                source_snapshot_date = $snapshotDate
                phase_out = $phaseOut
                catalog_sections = @($section)
                source_file = [System.IO.Path]::GetFileName($InputPath)
            }

            if (-not $productsByReference.ContainsKey($reference)) {
                $productsByReference[$reference] = $candidate
                continue
            }

            $existing = $productsByReference[$reference]
            $existing.catalog_sections = @($existing.catalog_sections + $section | Sort-Object -Unique)
            if ($null -eq $existing.price -and $null -ne $candidate.price) { $existing.price = $candidate.price }
            if ([string]::IsNullOrWhiteSpace($existing.brand)) { $existing.brand = $candidate.brand }
            if ([string]::IsNullOrWhiteSpace($existing.ncm)) { $existing.ncm = $candidate.ncm }
            if ([string]::IsNullOrWhiteSpace($existing.product_type)) { $existing.product_type = $candidate.product_type }
            if ([string]::IsNullOrWhiteSpace($existing.stock_text)) { $existing.stock_text = $candidate.stock_text }
            if ([string]::IsNullOrWhiteSpace($existing.source_url)) { $existing.source_url = $candidate.source_url }
            $existing.phase_out = [bool]($existing.phase_out -and $candidate.phase_out)
        }

        [System.Runtime.InteropServices.Marshal]::ReleaseComObject($usedRange) | Out-Null
        [System.Runtime.InteropServices.Marshal]::ReleaseComObject($worksheet) | Out-Null
    }
    $workbook.Close($false)
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($workbook) | Out-Null
}
finally {
    $excel.Quit()
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null
    [GC]::Collect()
    [GC]::WaitForPendingFinalizers()
}

$products = @($productsByReference.Values | Sort-Object reference)
$outputDirectory = Split-Path -Parent $OutputPath
New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
$products | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $OutputPath -Encoding UTF8
Write-Host "Coletek extraido: $($products.Count) produtos unicos -> $OutputPath"
