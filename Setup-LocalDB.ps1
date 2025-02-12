# VS Build Toolsインストーラーのダウンロード
Write-Host "Downloading VS Build Tools installer..."
Invoke-WebRequest -Uri https://aka.ms/vs/17/release/vs_buildtools.exe -OutFile vs_buildtools.exe
try {
    # LocalDBを含むSQL Server開発ツールのインストール
    Write-Host "Installing VS Build Tools with LocalDB..."
    $process = Start-Process -FilePath .\vs_buildtools.exe -ArgumentList `
        "--quiet", `
        "--wait", `
        "--norestart", `
        "--nocache", `
        "--installPath", "C:\BuildTools", `
        "--add", "Microsoft.VisualStudio.Workload.DataBuildTools", `
        "--add", "Microsoft.VisualStudio.Component.SQL.LocalDB.Runtime" `
        -Wait -PassThru

    if ($process.ExitCode -ne 0) {
        throw "Installation failed with exit code: $($process.ExitCode)"
    }
    
    # インストール確認
    Write-Host "Checking LocalDB installation..."
    $localDBPath = "C:\Program Files\Microsoft SQL Server\150\Tools\Binn\SqlLocalDB.exe"
    if (Test-Path $localDBPath) {
        & $localDBPath v
    } else {
        throw "LocalDB executable not found at expected path"
    }

    Write-Host "Installation completed successfully"
}
catch {
    Write-Host "An error occurred: $_"
    exit 1
}
finally {
    # インストーラーファイルの削除
    if (Test-Path .\vs_buildtools.exe) {
        Write-Host "Cleaning up installer file..."
        Remove-Item .\vs_buildtools.exe -Force
        
        if (Test-Path .\vs_buildtools.exe) {
            Write-Warning "Failed to remove installer file"
        } else {
            Write-Host "Installer file cleaned up successfully"
        }
    }
}