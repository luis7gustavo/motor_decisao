# Baixar e restaurar o pacote de coleta do GitHub

**Release:** https://github.com/luis7gustavo/motor_decisao/releases/tag/radar-coleta-carlos-2026-11-23

O release contém partes de um arquivo 7z criptografado e `PARTES-SHA256.csv`. O repositório é público; **a senha não está no GitHub**. Peça a senha a Luis por canal privado separado. O pacote aberto contém dumps de banco e credenciais C2: mantenha-o em disco protegido e não publique os arquivos extraídos.

## 1. Baixar

No release, baixe **todas** as partes chamadas `RadarLicita-Carlos-20261123-1244.7z.001`, `.002` e assim por diante, até a última, além de `PARTES-SHA256.csv`. Salve tudo na mesma pasta; não renomeie as partes. O arquivo completo extraído tinha 35,85 GB, portanto reserve espaço para as partes, a extração e a restauração dos bancos. Se a rede interromper algum download, baixe novamente só a parte afetada.

## 2. Verificar as partes

No PowerShell 7, ajuste o caminho e execute:

```powershell
$p = 'D:\RadarLicita-Carlos-Download'
$rows = Import-Csv -LiteralPath (Join-Path $p 'PARTES-SHA256.csv')
foreach ($row in $rows) {
    $f = Join-Path $p $row.Nome
    if (-not (Test-Path -LiteralPath $f)) { throw "Falta a parte $($row.Nome)" }
    if ((Get-Item -LiteralPath $f).Length -ne [long]$row.Bytes) { throw "Tamanho incorreto: $($row.Nome)" }
    if ((Get-FileHash -LiteralPath $f -Algorithm SHA256).Hash -ne $row.SHA256) { throw "Hash incorreto: $($row.Nome)" }
}
'Partes verificadas'
```

Confira também o SHA-256 do próprio `PARTES-SHA256.csv` recebido **por canal separado** de Luis. O manifesto dentro do pacote original tinha SHA-256 `F3D820BFBF16C59D231CD72591D3609D1E4A7E78D250710DB8128F5B88BF7441` antes da criptografia.

## 3. Abrir o arquivo criptografado

Instale 7-Zip no Windows (`winget install --id 7zip.7zip --exact`) ou use a versão oficial equivalente. No PowerShell, aponte para o executável e para a primeira parte:

```powershell
$seven = 'C:\Program Files\7-Zip\7z.exe'
$p = 'D:\RadarLicita-Carlos-Download'
& $seven t (Join-Path $p 'RadarLicita-Carlos-20261123-1244.7z.001')
& $seven x (Join-Path $p 'RadarLicita-Carlos-20261123-1244.7z.001') "-oD:\RadarLicita-Carlos-Extraido"
```

O 7-Zip pedirá a senha nas duas operações. Não digite a senha na linha de comando nem a salve no repositório. O teste do arquivo (`t`) e a extração (`x`) precisam terminar sem erro. Depois confira o pacote extraído:

```powershell
$pacote = 'D:\RadarLicita-Carlos-Extraido\RadarLicita-Carlos-20261123-1244'
& (Join-Path $pacote 'Importar-Coleta.ps1') -Pacote $pacote -Verificar
```

## 4. Restaurar com a coleta ainda parada

Leia `RECIBO_CORTE_CARLOS_20261123.md` e `TRANSFERENCIA_CARLOS.md` dentro do pacote. Crie um `.env` privado a partir de `CARLOS_ENV_MODELO.txt`, substituindo a senha marcada. O importador exige um destino Docker novo e deixa o worker parado. Execute a restauração conforme o roteiro e compare ledger, contagens e estado dos serviços antes de ativar um único worker. A origem de Luis deve continuar parada.

Se uma parte, hash, senha, dump ou serviço falhar, não inicie a coleta: preserve os arquivos e o destino para diagnóstico.

