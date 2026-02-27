
param(
  [int]$StartYear=2017,
  [int]$EndYear=2026,
  [string]$Years='',
  [string]$ForceYear='',
  [switch]$ForceAll,
  [switch]$SkipDownload,
  [string]$OutDataJs=(Join-Path $PSScriptRoot '..\data\data.js'),
  [string]$CacheDir=(Join-Path $PSScriptRoot '..\cache'),
  [string]$AliasFile=(Join-Path $PSScriptRoot '..\data\measure_aliases.json')
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem
Add-Type -AssemblyName Microsoft.VisualBasic
Add-Type -AssemblyName System.Security

$ParserVersion='v2.0-incremental-canonical-name'
$YearSources=@{
  2026='https://www.cms.gov/files/zip/2026-star-ratings-data-tables.zip';
  2025='https://www.cms.gov/files/zip/2025-star-ratings-data-tables.zip';
  2024='https://www.cms.gov/files/zip/2024-star-ratings-data-tables-jul-2-2024.zip';
  2023='https://www.cms.gov/files/zip/2023-star-ratings-and-display-measures.zip';
  2022='https://www.cms.gov/files/zip/2022-star-ratings-and-display-measures.zip';
  2021='https://www.cms.gov/files/zip/2021-star-ratings-and-display-measures.zip';
  2020='https://www.cms.gov/files/zip/2020-star-ratings-and-display-measures.zip';
  2019='https://www.cms.gov/files/zip/2019-star-ratings-and-display-measures.zip';
  2018='https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/2018-star-ratings-and-display-measures.zip';
  2017='https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/2017_star_ratings_and_display_measures.zip'
}

function New-Dir([string]$p){if(-not(Test-Path $p)){New-Item -ItemType Directory -Path $p -Force|Out-Null}}
function Parse-IntList([string]$t){if([string]::IsNullOrWhiteSpace($t)){return @()};$o=@();foreach($x in ($t -split '[,;\s]+'|?{$_})){[int]$n=0;if(-not[int]::TryParse($x,[ref]$n)){throw "Invalid year '$x'"};$o+=$n};return @($o|Sort-Object -Unique)}
function Get-FileHashHex([string]$p){if(-not(Test-Path $p)){return ''};$sha=[System.Security.Cryptography.SHA256]::Create();try{$s=[IO.File]::OpenRead($p);try{$h=$sha.ComputeHash($s);return(([BitConverter]::ToString($h)-replace'-','').ToLowerInvariant())}finally{$s.Dispose()}}finally{$sha.Dispose()}}
function Normalize-MeasureName([string]$n){if([string]::IsNullOrWhiteSpace($n)){return ''};$n=$n.Trim().ToLowerInvariant();$n=$n -replace '[’‘`´]','\''';$n=$n -replace '&',' and ';$n=$n -replace '[^a-z0-9\s]',' ';$n=$n -replace '\s+',' ';return $n.Trim()}
function Parse-Number([string]$v){if([string]::IsNullOrWhiteSpace($v)){return $null};$v=$v.Trim();if($v -match '^(?i)(na|n/a|--|suppressed|not available|not enough data available|no data available|plan too small to be measured|plan not required to report measure|not required to report)$'){return $null};$v=$v -replace ',','';if($v -match '^[-+]?\d+(\.\d+)?%$'){return [double]($v.TrimEnd('%'))};if($v -match '^\(([-\d\.]+)\)$'){$v='-'+$Matches[1]};if($v -match '^[-+]?\d+(\.\d+)?$'){return [double]$v};return $null}
function Get-WeightedAverage([object[]]$rows,[string]$metric,[string]$w){$v=@($rows|?{$null -ne $_.$metric -and $null -ne $_.$w -and $_.$w -gt 0});if($v.Count -eq 0){return $null};$den=($v|Measure-Object -Property $w -Sum).Sum;if($den -le 0){return $null};$num=($v|%{[double]$_.($w)*[double]$_.($metric)}|Measure-Object -Sum).Sum;return [double]$num/[double]$den}

function Get-ZipEntries([string]$zipPath){$z=[IO.Compression.ZipFile]::OpenRead($zipPath);try{return @($z.Entries|% FullName)}finally{$z.Dispose()}}
function Extract-ZipEntryToFile([string]$zipPath,[string]$entryName,[string]$outFile){$z=[IO.Compression.ZipFile]::OpenRead($zipPath);try{$e=$z.GetEntry($entryName);if(-not$e){throw "Entry not found: $entryName"};$ins=$e.Open();try{$outs=[IO.File]::Open($outFile,[IO.FileMode]::Create,[IO.FileAccess]::Write);try{$ins.CopyTo($outs)}finally{$outs.Dispose()}}finally{$ins.Dispose()}}finally{$z.Dispose()}}
function Select-LatestEntry([string[]]$entries){if($entries.Count -eq 0){return $null};return ($entries|Sort-Object -Descending|Select-Object -First 1)}
function Resolve-TableEntryInZip([string]$zipPath,[string]$kind){
  $pats=if($kind -eq 'measure_data'){@('(?i)measure data','(?i)_data\.csv$')}else{@('(?i)measure stars','(?i)_stars\.csv$')}
  $pick={
    param([string[]]$es)
    $csv=@($es|?{$_ -match '(?i)\.csv$' -and $_ -notmatch '(?i)display[_ ]measures?'})
    foreach($p in $pats){
      $h=@($csv|?{$_ -match $p})
      if($h.Count -gt 0){return (Select-LatestEntry $h)}
    }
    return $null
  }
  $es=Get-ZipEntries $zipPath
  $d=&$pick $es
  if($d){return [pscustomobject]@{isInner=$false;inner='';csv=$d}}

  $inners=@(
    $es |
      ?{
        $_ -match '(?i)\.zip$' -and
        $_ -notmatch '(?i)display[_ ]measures?' -and
        ($_ -match '(?i)star ratings data|report card master table|part c and d medicare star ratings data')
      }
  )
  if($inners.Count -eq 0){$inners=@($es|?{$_ -match '(?i)\.zip$' -and $_ -notmatch '(?i)display[_ ]measures?'})}
  if($inners.Count -eq 0){return $null}

  $inner=Select-LatestEntry $inners
  $tmp=Join-Path $env:TEMP ('inner_'+[guid]::NewGuid()+'.zip')
  Extract-ZipEntryToFile $zipPath $inner $tmp
  try{
    $ies=Get-ZipEntries $tmp
    $ic=&$pick $ies
    if(-not$ic){return $null}
    return [pscustomobject]@{isInner=$true;inner=$inner;csv=$ic}
  }finally{
    if(Test-Path $tmp){Remove-Item $tmp -Force -ErrorAction SilentlyContinue}
  }
}
function Extract-ResolvedCsv([string]$zipPath,[object]$resolved,[string]$outPath){if(-not$resolved.isInner){Extract-ZipEntryToFile $zipPath $resolved.csv $outPath;return};$tmp=Join-Path $env:TEMP ('inner_'+[guid]::NewGuid()+'.zip');try{Extract-ZipEntryToFile $zipPath $resolved.inner $tmp;Extract-ZipEntryToFile $tmp $resolved.csv $outPath}finally{if(Test-Path $tmp){Remove-Item $tmp -Force -ErrorAction SilentlyContinue}}}
function Get-EnrollmentZipCandidates([int]$y){@("https://www.cms.gov/files/zip/monthly-enrollment-contract-february-$y.zip","https://www.cms.gov/files/zip/monthly-report-contract-february-$y.zip","https://www.cms.gov/files/zip/monthly-report-by-contract-february-$y.zip","https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/$y/feb/monthly-report-by-contract-$y-02.zip")}
function Resolve-EnrollmentCsvEntry([string]$zipPath){$es=Get-ZipEntries $zipPath;$csv=@($es|?{$_ -match '(?i)\.csv$' -and $_ -match '(?i)monthly[_ -].*contract|report[_ -]by[_ -]contract'}|Sort-Object);if($csv.Count -eq 0){throw "No enrollment CSV entry found in $zipPath"};return $csv[0]}
function Read-CsvRows([string]$csv){$rows=New-Object 'System.Collections.Generic.List[object]';$p=New-Object Microsoft.VisualBasic.FileIO.TextFieldParser($csv);try{$p.TextFieldType=[Microsoft.VisualBasic.FileIO.FieldType]::Delimited;$p.SetDelimiters(',');$p.HasFieldsEnclosedInQuotes=$true;while(-not$p.EndOfData){$rows.Add($p.ReadFields())}}finally{$p.Close()};return $rows}
function Parse-MeasureTable([int]$year,[string]$csvPath,[string]$valueField,[hashtable]$aliasMap,[hashtable]$canonDisplay,[hashtable]$yearCanonCodes){
  $rows=Read-CsvRows $csvPath; if($rows.Count -eq 0){throw "Empty CSV: $csvPath"}
  $hdr=$null;for($i=0;$i -lt [Math]::Min(30,$rows.Count);$i++){if($rows[$i].Length -gt 0 -and ($rows[$i][0]|Out-String).Trim() -eq 'CONTRACT_ID'){$hdr=$i;break}}
  if($null -eq $hdr){throw "CONTRACT_ID header not found: $csvPath"}
  $first=$null;for($i=$hdr+1;$i -lt $rows.Count;$i++){if($rows[$i].Length -eq 0){continue};$id=($rows[$i][0]|Out-String).Trim();if($id -match '^[A-Z]\d{4}$'){$first=$i;break}}
  if($null -eq $first){throw "No contract rows found: $csvPath"}

  $headerRows=@($rows[$hdr..($first-1)]);$cc=0;foreach($r in $headerRows){if($r.Length -gt $cc){$cc=$r.Length}}
  $cols=New-Object 'System.Collections.Generic.List[object]'
  for($c=0;$c -lt $cc;$c++){
    $parts=New-Object 'System.Collections.Generic.List[string]'
    foreach($hr in $headerRows){$v=if($c -lt $hr.Length){($hr[$c]|Out-String).Trim()}else{''};if([string]::IsNullOrWhiteSpace($v)){continue};if($parts.Count -gt 0 -and $parts[$parts.Count-1] -eq $v){continue};$parts.Add($v)}
    $joined=(($parts -join ' | ') -replace '\s+',' ').Trim();$code='';$name=''
    foreach($p in $parts){if($p -match '(?i)\b(?<code>[A-Z]\d{2}[A-Z]?)\s*:\s*(?<name>.+)$'){$code=$Matches.code.ToUpperInvariant();$name=$Matches.name.Trim();break}}
    if(-not[string]::IsNullOrWhiteSpace($name)){
      $norm=Normalize-MeasureName $name
      $canon=if($aliasMap.ContainsKey($norm)){[string]$aliasMap[$norm]}else{$norm}
      if(-not$canonDisplay.ContainsKey($canon)){$canonDisplay[$canon]=$name}
      if(-not$yearCanonCodes.ContainsKey($canon)){$yearCanonCodes[$canon]=@{}}
      if(-not[string]::IsNullOrWhiteSpace($code)){$yearCanonCodes[$canon][$code]=$true}
      $cols.Add([pscustomobject]@{i=$c;j=$joined;code=$code;raw=$name;norm=$norm;canon=$canon})
    } else {
      $cols.Add([pscustomobject]@{i=$c;j=$joined;code='';raw='';norm='';canon=''})
    }
  }

  $orgCol=($cols|?{$_.j -match '(?i)Organization Marketing Name'}|Select-Object -First 1)
  $parCol=($cols|?{$_.j -match '(?i)Parent Organization'}|Select-Object -First 1)
  $orgIdx=if($orgCol){[int]$orgCol.i}else{$null};$parIdx=if($parCol){[int]$parCol.i}else{$null}
  $mCols=@($cols|?{-not[string]::IsNullOrWhiteSpace($_.canon)})

  $out=@{}
  foreach($row in $rows[$first..($rows.Count-1)]){
    if($row.Length -eq 0){continue}
    $contract=($row[0]|Out-String).Trim(); if($contract -notmatch '^[A-Z]\d{4}$'){continue}
    $prefix=$contract.Substring(0,1); if(@('H','R','E') -notcontains $prefix){continue}

    if(-not$out.ContainsKey($contract)){
      $org=if($null -ne $orgIdx -and $row.Length -gt $orgIdx){($row[$orgIdx]|Out-String).Trim()}else{''}
      $par=if($null -ne $parIdx -and $row.Length -gt $parIdx){($row[$parIdx]|Out-String).Trim()}else{''}
      $out[$contract]=[pscustomobject]@{rating_year=$year;contract_id=$contract;org_marketing_name=$org;parent_organization=$par;measures=@{}}
    }

    foreach($mc in $mCols){
      if($row.Length -le $mc.i){continue}
      $v=Parse-Number (($row[$mc.i]|Out-String).Trim())
      if(-not$out[$contract].measures.ContainsKey($mc.canon)){
        $out[$contract].measures[$mc.canon]=[pscustomobject]@{measure_name_raw=$mc.raw;measure_name_normalized=$mc.norm;measure_name_canonical_key=$mc.canon;measure_code_observed=@();raw_measure_data=$null;measure_stars=$null;star_weight=1.0}
      }
      $m=$out[$contract].measures[$mc.canon]
      if(-not[string]::IsNullOrWhiteSpace($mc.code) -and (@($m.measure_code_observed) -notcontains $mc.code)){$m.measure_code_observed+=$mc.code}
      if($null -eq $m.$valueField -and $null -ne $v){$m.$valueField=$v}
    }
  }
  return $out
}

function Get-EnrollmentByContract([string]$csvPath){
  $rows=Import-Csv $csvPath; $out=@{}; if($rows.Count -eq 0){return $out}
  $names=@($rows[0].PSObject.Properties.Name)
  $contractCol=$names|?{$_ -match '(?i)^Contract Number$|^Contract ID$|^Contract$'}|Select-Object -First 1
  $enrollCol=$names|?{$_ -match '(?i)^Total Enrollment$|Enrollment'}|Select-Object -First 1
  $planTypeCol=$names|?{$_ -match '(?i)Plan Type|Contract Type|Type'}|Select-Object -First 1
  if(-not$contractCol -or -not$enrollCol){return $out}

  foreach($r in $rows){
    $contract=(($r.$contractCol|Out-String).Trim()); if($contract -notmatch '^[A-Z]\d{4}$'){continue}
    $prefix=$contract.Substring(0,1); if(@('H','R','E') -notcontains $prefix){continue}
    if($planTypeCol){$pt=(($r.$planTypeCol|Out-String).Trim()); if($pt -match '(?i)pdp|stand[- ]alone'){continue}}
    $members=Parse-Number (($r.$enrollCol|Out-String).Trim()); if($null -eq $members -or $members -le 0){continue}
    if(-not$out.ContainsKey($contract)){$out[$contract]=0};$out[$contract]+=[int][math]::Round($members,0)
  }
  return $out
}

$selectedYears=if([string]::IsNullOrWhiteSpace($Years)){@($StartYear..$EndYear)}else{Parse-IntList $Years}
$selectedYears=@($selectedYears|?{$_ -ge $StartYear -and $_ -le $EndYear}|Sort-Object -Unique)
if($selectedYears.Count -eq 0){throw 'No selected years to process.'}
$forceYears=Parse-IntList $ForceYear

$rawCache=Join-Path $CacheDir 'raw';$yearCache=Join-Path $CacheDir 'years';$manifestPath=Join-Path $CacheDir 'manifest.json'
New-Dir $CacheDir;New-Dir $rawCache;New-Dir $yearCache

$aliasMap=@{};$aliasHash=''
if(Test-Path $AliasFile){$aliasHash=Get-FileHashHex $AliasFile;$a=Get-Content -Raw $AliasFile|ConvertFrom-Json;foreach($p in $a.PSObject.Properties){$k=Normalize-MeasureName $p.Name;$v=Normalize-MeasureName ([string]$p.Value);if($k -and $v){$aliasMap[$k]=$v}}}

$manifest=@{parser_version=$ParserVersion;alias_hash=$aliasHash;years=@{}}
if(Test-Path $manifestPath){
  try{
    $old=Get-Content -Raw $manifestPath|ConvertFrom-Json
    if($old -and $old.years){
      $manifest=@{parser_version=[string]$old.parser_version;alias_hash=[string]$old.alias_hash;years=@{}}
      foreach($yp in $old.years.PSObject.Properties){
        $manifest.years[$yp.Name]=@{}
        foreach($fp in $yp.Value.PSObject.Properties){$manifest.years[$yp.Name][$fp.Name]=$fp.Value}
      }
    }
  }catch{Write-Warning 'Could not parse existing manifest. Recreating.'}
}
if(-not$manifest.ContainsKey('years')){$manifest['years']=@{}}

$tmp=Join-Path $env:TEMP ('stars_inc_'+[guid]::NewGuid());New-Dir $tmp
$buildStatus=New-Object 'System.Collections.Generic.List[object]'
try {
  foreach($year in $selectedYears){
    if(-not$YearSources.ContainsKey($year)){Write-Warning "Skipping ${year}: no source configured.";continue}
    $yk=[string]$year;$yearDir=Join-Path $rawCache $yk;New-Dir $yearDir
    $shardPath=Join-Path $yearCache ("$year.json")
    $forced=$ForceAll.IsPresent -or (@($forceYears) -contains $year)
    $my=if($manifest.years.ContainsKey($yk)){$manifest.years[$yk]}else{$null}

    $canSkip=$false
    if(-not$forced -and (Test-Path $shardPath) -and $my){if($my.parser_version -eq $ParserVersion -and $my.alias_hash -eq $aliasHash -and $my.star_url -eq $YearSources[$year]){$canSkip=$true}}
    if($canSkip){Write-Host "[$year] Using cached shard (fingerprint match).";$buildStatus.Add([pscustomobject]@{year=$year;action='cached';shard=$shardPath});continue}

    $starZip=Join-Path $yearDir 'stars.zip'
    $needStars=$forced -or -not(Test-Path $starZip)
    if($needStars){if($SkipDownload.IsPresent -and -not(Test-Path $starZip)){throw "[$year] SkipDownload set and missing stars.zip cache."};if(-not$SkipDownload.IsPresent){Write-Host "[$year] Downloading stars ZIP...";Invoke-WebRequest -Uri $YearSources[$year] -OutFile $starZip -TimeoutSec 300}}

    $resData=Resolve-TableEntryInZip $starZip 'measure_data';if(-not$resData){throw "[$year] Could not resolve measure_data entry."}
    $resStars=Resolve-TableEntryInZip $starZip 'measure_stars';if(-not$resStars){throw "[$year] Could not resolve measure_stars entry."}
    $dataCsv=Join-Path $tmp ("$year.measure_data.csv");$starsCsv=Join-Path $tmp ("$year.measure_stars.csv")
    Extract-ResolvedCsv $starZip $resData $dataCsv;Extract-ResolvedCsv $starZip $resStars $starsCsv

    $enrollZip=Join-Path $yearDir 'enrollment.zip';$enrollUrl=if($my -and $my.enrollment_url){[string]$my.enrollment_url}else{''}
    $needEnroll=$forced -or -not(Test-Path $enrollZip)
    if($needEnroll){
      if($SkipDownload.IsPresent -and -not(Test-Path $enrollZip)){throw "[$year] SkipDownload set and missing enrollment.zip cache."}
      if(-not$SkipDownload.IsPresent){
        Write-Host "[$year] Downloading enrollment ZIP..."
        $cands=if($enrollUrl){@($enrollUrl)+@(Get-EnrollmentZipCandidates $year)}else{@(Get-EnrollmentZipCandidates $year)}
        $enrollUrl=''
        foreach($c in ($cands|Select-Object -Unique)){try{Invoke-WebRequest -Uri $c -OutFile $enrollZip -TimeoutSec 300;$enrollUrl=$c;break}catch{}}
        if(-not$enrollUrl){throw "[$year] Could not download enrollment ZIP."}
      }
    }
    if(-not$enrollUrl -and $my -and $my.enrollment_url){$enrollUrl=[string]$my.enrollment_url}
    if(-not$enrollUrl){$enrollUrl=(Get-EnrollmentZipCandidates $year)[0]}

    $enrollEntry=Resolve-EnrollmentCsvEntry $enrollZip
    $enrollCsv=Join-Path $tmp ("$year.enrollment.csv");Extract-ZipEntryToFile $enrollZip $enrollEntry $enrollCsv

    $canonDisplay=@{};$yearCanonCodes=@{}
    $rawByContract=Parse-MeasureTable $year $dataCsv 'raw_measure_data' $aliasMap $canonDisplay $yearCanonCodes
    $starsByContract=Parse-MeasureTable $year $starsCsv 'measure_stars' $aliasMap $canonDisplay $yearCanonCodes
    $enrollByContract=Get-EnrollmentByContract $enrollCsv

    $contractRecords=New-Object 'System.Collections.Generic.List[object]'
    $contractTotals=New-Object 'System.Collections.Generic.List[object]'

    foreach($contract in $rawByContract.Keys){
      if(-not$enrollByContract.ContainsKey($contract)){continue}
      $lives=[int]$enrollByContract[$contract];if($lives -le 0){continue}
      $rec=$rawByContract[$contract]

      if($starsByContract.ContainsKey($contract)){
        foreach($k in $starsByContract[$contract].measures.Keys){
          if(-not$rec.measures.ContainsKey($k)){$rec.measures[$k]=[pscustomobject]@{measure_name_raw=$starsByContract[$contract].measures[$k].measure_name_raw;measure_name_normalized=$starsByContract[$contract].measures[$k].measure_name_normalized;measure_name_canonical_key=$k;measure_code_observed=@();raw_measure_data=$null;measure_stars=$null;star_weight=1.0}}
          $rec.measures[$k].measure_stars=$starsByContract[$contract].measures[$k].measure_stars
          foreach($cc in @($starsByContract[$contract].measures[$k].measure_code_observed)){if(@($rec.measures[$k].measure_code_observed) -notcontains $cc){$rec.measures[$k].measure_code_observed+=$cc}}
        }
      }

      $wRows=@($rec.measures.Values|?{$null -ne $_.measure_stars -and $null -ne $_.star_weight});$sumW=if($wRows.Count -gt 0){($wRows|Measure-Object -Property star_weight -Sum).Sum}else{0}
      $total=0.0
      foreach($m in $rec.measures.Values){
        $score=$null;if($sumW -gt 0 -and $null -ne $m.measure_stars -and $null -ne $m.star_weight){$score=([double]$m.measure_stars*[double]$m.star_weight)/[double]$sumW;$total+=[double]$score}
        $contractRecords.Add([pscustomobject]@{rating_year=$year;contract_id=$contract;org_marketing_name=$rec.org_marketing_name;parent_organization=$rec.parent_organization;measure_name_raw=$m.measure_name_raw;measure_name_normalized=$m.measure_name_normalized;measure_name_canonical=if($canonDisplay.ContainsKey($m.measure_name_canonical_key)){$canonDisplay[$m.measure_name_canonical_key]}else{$m.measure_name_raw};measure_name_canonical_key=$m.measure_name_canonical_key;measure_code_observed=(@($m.measure_code_observed|Sort-Object -Unique)-join '|');raw_measure_data=$m.raw_measure_data;measure_stars=$m.measure_stars;star_weight=$m.star_weight;sum_available_weights_contract_year=[double]$sumW;calculated_raw_stars_score=$score;total_raw_stars_score_contract_year=$null;enrollment_lives=$lives})
      }
      $contractTotals.Add([pscustomobject]@{rating_year=$year;contract_id=$contract;org_marketing_name=$rec.org_marketing_name;parent_organization=$rec.parent_organization;enrollment_lives=$lives;total_raw_stars_score_contract_year=[double]$total})
    }

    $totalsByContract=@{};foreach($t in $contractTotals){$totalsByContract[$t.contract_id]=[double]$t.total_raw_stars_score_contract_year}
    foreach($r in $contractRecords){if($totalsByContract.ContainsKey($r.contract_id)){$r.total_raw_stars_score_contract_year=$totalsByContract[$r.contract_id]}}

    $parentAgg=New-Object 'System.Collections.Generic.List[object]'
    foreach($g in ($contractRecords|Group-Object rating_year,measure_name_canonical_key,parent_organization)){$rows=@($g.Group);if($rows.Count -eq 0){continue};$parentAgg.Add([pscustomobject]@{rating_year=[int]$rows[0].rating_year;parent_organization=[string]$rows[0].parent_organization;measure_name_canonical=[string]$rows[0].measure_name_canonical;measure_name_canonical_key=[string]$rows[0].measure_name_canonical_key;measure_code_observed=((@($rows.measure_code_observed|?{$_}|Sort-Object -Unique)-join '|'));weighted_raw_measure_data=Get-WeightedAverage $rows 'raw_measure_data' 'enrollment_lives';weighted_measure_stars=Get-WeightedAverage $rows 'measure_stars' 'enrollment_lives';weighted_star_weight=Get-WeightedAverage $rows 'star_weight' 'enrollment_lives';weighted_calculated_raw_stars_score=Get-WeightedAverage $rows 'calculated_raw_stars_score' 'enrollment_lives';members_included=($rows|Measure-Object -Property enrollment_lives -Sum).Sum;contracts_included=(@($rows.contract_id|Sort-Object -Unique)).Count})}

    $allAgg=New-Object 'System.Collections.Generic.List[object]'
    foreach($g in ($contractRecords|Group-Object rating_year,measure_name_canonical_key)){$rows=@($g.Group);if($rows.Count -eq 0){continue};$allAgg.Add([pscustomobject]@{rating_year=[int]$rows[0].rating_year;scope='all_ma';measure_name_canonical=[string]$rows[0].measure_name_canonical;measure_name_canonical_key=[string]$rows[0].measure_name_canonical_key;measure_code_observed=((@($rows.measure_code_observed|?{$_}|Sort-Object -Unique)-join '|'));weighted_raw_measure_data=Get-WeightedAverage $rows 'raw_measure_data' 'enrollment_lives';weighted_measure_stars=Get-WeightedAverage $rows 'measure_stars' 'enrollment_lives';weighted_star_weight=Get-WeightedAverage $rows 'star_weight' 'enrollment_lives';weighted_calculated_raw_stars_score=Get-WeightedAverage $rows 'calculated_raw_stars_score' 'enrollment_lives';members_included=($rows|Measure-Object -Property enrollment_lives -Sum).Sum;contracts_included=(@($rows.contract_id|Sort-Object -Unique)).Count})}

    $parentTotals=New-Object 'System.Collections.Generic.List[object]';foreach($g in ($contractTotals|Group-Object rating_year,parent_organization)){$rows=@($g.Group);$parentTotals.Add([pscustomobject]@{rating_year=[int]$rows[0].rating_year;parent_organization=[string]$rows[0].parent_organization;weighted_total_raw_stars_score=Get-WeightedAverage $rows 'total_raw_stars_score_contract_year' 'enrollment_lives';members_included=($rows|Measure-Object -Property enrollment_lives -Sum).Sum;contracts_included=(@($rows.contract_id|Sort-Object -Unique)).Count})}
    $allTotals=New-Object 'System.Collections.Generic.List[object]';foreach($g in ($contractTotals|Group-Object rating_year)){$rows=@($g.Group);$allTotals.Add([pscustomobject]@{rating_year=[int]$rows[0].rating_year;scope='all_ma';weighted_total_raw_stars_score=Get-WeightedAverage $rows 'total_raw_stars_score_contract_year' 'enrollment_lives';members_included=($rows|Measure-Object -Property enrollment_lives -Sum).Sum;contracts_included=(@($rows.contract_id|Sort-Object -Unique)).Count})}

    $multiCodes=New-Object 'System.Collections.Generic.List[object]';foreach($k in $yearCanonCodes.Keys){$codes=@($yearCanonCodes[$k].Keys|Sort-Object);if($codes.Count -gt 1){$multiCodes.Add([pscustomobject]@{rating_year=$year;measure_name_canonical_key=$k;measure_codes=($codes -join '|')})}}
    $h5216=@($contractRecords|?{$_.rating_year -eq 2026 -and $_.contract_id -eq 'H5216' -and $_.measure_name_canonical_key -match 'members choosing to leave the plan'})
    $known=[pscustomobject]@{contract_id='H5216';rating_year=2026;canonical_measure_name_like='members choosing to leave the plan';values_found=@($h5216|Select-Object -ExpandProperty raw_measure_data);check_pass=((@($h5216|?{$_.raw_measure_data -eq 19})).Count -gt 0)}

    $shard=@{metadata=@{year=$year;parser_version=$ParserVersion;alias_hash=$aliasHash;star_url=$YearSources[$year];enrollment_url=$enrollUrl;measure_data_source_file=if($resData.isInner){"$($resData.inner) :: $($resData.csv)"}else{$resData.csv};measure_stars_source_file=if($resStars.isInner){"$($resStars.inner) :: $($resStars.csv)"}else{$resStars.csv};enrollment_source_file=$enrollEntry;built_at_utc=[datetime]::UtcNow.ToString('o')};contract_records=$contractRecords.ToArray();parent_aggregates=$parentAgg.ToArray();all_ma_aggregates=$allAgg.ToArray();contract_year_totals=$contractTotals.ToArray();parent_year_totals=$parentTotals.ToArray();all_ma_year_totals=$allTotals.ToArray();diagnostics=@{same_canonical_multiple_codes=$multiCodes.ToArray();known_case_h5216_2026=$known}}
    Set-Content -Path $shardPath -Value ($shard|ConvertTo-Json -Depth 10 -Compress) -Encoding UTF8

    $manifest.years[$yk]=@{parser_version=$ParserVersion;alias_hash=$aliasHash;star_url=$YearSources[$year];enrollment_url=$enrollUrl;measure_data_source_file=if($resData.isInner){"$($resData.inner) :: $($resData.csv)"}else{$resData.csv};measure_stars_source_file=if($resStars.isInner){"$($resStars.inner) :: $($resStars.csv)"}else{$resStars.csv};enrollment_source_file=$enrollEntry;fingerprint="${ParserVersion}|${aliasHash}|$($YearSources[$year])|$enrollUrl|$($resData.csv)|$($resStars.csv)|$enrollEntry";shard_path=$shardPath;last_built_utc=[datetime]::UtcNow.ToString('o')}
    Write-Host "[$year] Rebuilt shard: $shardPath"; $buildStatus.Add([pscustomobject]@{year=$year;action='rebuilt';shard=$shardPath})
  }

  $manifest.parser_version=$ParserVersion;$manifest.alias_hash=$aliasHash;Set-Content -Path $manifestPath -Value ($manifest|ConvertTo-Json -Depth 8) -Encoding UTF8

  $mergeYears=@($StartYear..$EndYear)
  $allContracts=New-Object 'System.Collections.Generic.List[object]';$allParents=New-Object 'System.Collections.Generic.List[object]';$allAllMa=New-Object 'System.Collections.Generic.List[object]';$allContractTotals=New-Object 'System.Collections.Generic.List[object]';$allParentTotals=New-Object 'System.Collections.Generic.List[object]';$allAllMaTotals=New-Object 'System.Collections.Generic.List[object]';$allDiagnostics=New-Object 'System.Collections.Generic.List[object]';$yearBuildSummary=New-Object 'System.Collections.Generic.List[object]'

  foreach($y in $mergeYears){$sp=Join-Path $yearCache ("$y.json");if(-not(Test-Path $sp)){Write-Warning "[$y] Missing shard during merge; skipping year.";$yearBuildSummary.Add([pscustomobject]@{year=$y;status='missing_shard'});continue};$yp=Get-Content -Raw $sp|ConvertFrom-Json;foreach($r in @($yp.contract_records)){$allContracts.Add($r)};foreach($r in @($yp.parent_aggregates)){$allParents.Add($r)};foreach($r in @($yp.all_ma_aggregates)){$allAllMa.Add($r)};foreach($r in @($yp.contract_year_totals)){$allContractTotals.Add($r)};foreach($r in @($yp.parent_year_totals)){$allParentTotals.Add($r)};foreach($r in @($yp.all_ma_year_totals)){$allAllMaTotals.Add($r)};$allDiagnostics.Add([pscustomobject]@{year=$y;diagnostics=$yp.diagnostics});$s=$buildStatus|?{$_.year -eq $y}|Select-Object -First 1;if($s){$yearBuildSummary.Add([pscustomobject]@{year=$y;status=$s.action})}else{$yearBuildSummary.Add([pscustomobject]@{year=$y;status='from_cache'})}}
  $codeToCanon=@{}
  foreach($r in $allContracts){if(-not$r.measure_code_observed){continue};foreach($c in ([string]$r.measure_code_observed -split '\|')){if(-not$c){continue};if(-not$codeToCanon.ContainsKey($c)){$codeToCanon[$c]=@{}};$codeToCanon[$c][[string]$r.measure_name_canonical_key]=$true}}
  $codeDrift=New-Object 'System.Collections.Generic.List[object]'
  foreach($c in ($codeToCanon.Keys|Sort-Object)){$ks=@($codeToCanon[$c].Keys|Sort-Object);if($ks.Count -gt 1){$codeDrift.Add([pscustomobject]@{measure_code=$c;canonical_keys=($ks -join '|')})}}

  $yearsOut=@($allContracts|Select-Object -ExpandProperty rating_year -Unique|Sort-Object)
  $payload=@{metadata=@{generated_at_utc=[datetime]::UtcNow.ToString('o');years=$yearsOut;parser_version=$ParserVersion;alias_hash=$aliasHash;score_formula='(measure_stars * star_weight) / sum_available_weights(contract-year, excluding unavailable measures)';total_score_formula='sum(calculated_raw_stars_score) across available measures in contract-year';star_weight_note='CMS measure tables do not expose explicit star weights; this build uses equal weights (1.0) per available measure.';source_urls_by_year=($manifest.years);build_status_by_year=$yearBuildSummary.ToArray();diagnostics=@{code_to_canonical_drift=$codeDrift.ToArray();per_year=$allDiagnostics.ToArray()}};contract_records=$allContracts.ToArray();parent_aggregates=$allParents.ToArray();all_ma_aggregates=$allAllMa.ToArray();contract_year_totals=$allContractTotals.ToArray();parent_year_totals=$allParentTotals.ToArray();all_ma_year_totals=$allAllMaTotals.ToArray()}

  $outDir=Split-Path -Parent $OutDataJs; New-Dir $outDir
  Set-Content -Path $OutDataJs -Value ("window.STARS_DATA = " + ($payload|ConvertTo-Json -Depth 12 -Compress) + ';') -Encoding UTF8

  Write-Host "Wrote: $OutDataJs"
  Write-Host "Contract rows: $($allContracts.Count)"
  Write-Host "Parent rows: $($allParents.Count)"
  Write-Host "All-MA rows: $($allAllMa.Count)"
  Write-Host "Contract-year totals rows: $($allContractTotals.Count)"
}
finally {
  if(Test-Path $tmp){Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue}
}
