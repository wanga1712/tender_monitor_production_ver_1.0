# PowerShell script for deploying updates to Linux server
# Usage: .\deploy_to_server.ps1

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Transfer TenderMonitor updates to server" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# Variables
$ProjectDir = "C:\Users\wangr\PycharmProjects\pythonProject97"
$ServerHost = "nyx"
$ArchiveName = "updates.tar.gz"

# Change to project directory
Set-Location $ProjectDir
Write-Host "Working directory: $ProjectDir" -ForegroundColor Green
Write-Host ""

# Create archive with updated files
Write-Host "Creating archive with updated files..." -ForegroundColor Yellow
try {
    # Check if tar is available
    $tarExists = Get-Command tar -ErrorAction SilentlyContinue
    
    if ($tarExists) {
        tar -czf $ArchiveName orchestration/monitoring_service.py parsing_xml/okpd_parser.py file_downloader.py eis_requester.py config.ini
        Write-Host "   Archive created with tar" -ForegroundColor Green
    }
    else {
        # Use Compress-Archive if tar is not available
        Compress-Archive -Path orchestration/monitoring_service.py, parsing_xml/okpd_parser.py, file_downloader.py, eis_requester.py -DestinationPath "updates.zip" -Force
        $ArchiveName = "updates.zip"
        Write-Host "   Archive created with Compress-Archive" -ForegroundColor Green
    }
}
catch {
    Write-Host "   Error creating archive: $_" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Check archive size
$archiveSize = (Get-Item $ArchiveName).Length
Write-Host "Archive size: $([math]::Round($archiveSize/1KB, 2)) KB" -ForegroundColor Cyan
Write-Host ""

# Transfer archive to server
Write-Host "Transferring archive to server $ServerHost..." -ForegroundColor Yellow
try {
    scp $ArchiveName "${ServerHost}:/tmp/"
    Write-Host "   Archive successfully transferred to server" -ForegroundColor Green
}
catch {
    Write-Host "   Error transferring archive: $_" -ForegroundColor Red
    Write-Host "   Make sure SSH is configured and server $ServerHost is accessible" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Transfer deployment script to server
if (Test-Path "deploy_updates.sh") {
    Write-Host "Transferring deployment script to server..." -ForegroundColor Yellow
    try {
        scp deploy_updates.sh "${ServerHost}:/tmp/"
        Write-Host "   Deployment script transferred" -ForegroundColor Green
    }
    catch {
        Write-Host "   Could not transfer deployment script" -ForegroundColor Yellow
    }
    Write-Host ""
}

# Remove local archive
Write-Host "Removing local archive..." -ForegroundColor Yellow
Remove-Item $ArchiveName -Force
Write-Host "   Local archive removed" -ForegroundColor Green
Write-Host ""

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Files successfully transferred to server!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Connect to server:" -ForegroundColor White
Write-Host "   ssh $ServerHost" -ForegroundColor Cyan
Write-Host ""
Write-Host "2. Run deployment script:" -ForegroundColor White
Write-Host "   chmod +x /tmp/deploy_updates.sh" -ForegroundColor Cyan
Write-Host "   sudo /tmp/deploy_updates.sh" -ForegroundColor Cyan
Write-Host ""
Write-Host "   OR run commands manually:" -ForegroundColor White
Write-Host "   cd /opt/tendermonitor" -ForegroundColor Cyan
Write-Host "   sudo systemctl stop tendermonitor.service" -ForegroundColor Cyan
Write-Host "   tar -xzf /tmp/$ArchiveName" -ForegroundColor Cyan
Write-Host "   sudo systemctl start tendermonitor.service" -ForegroundColor Cyan
Write-Host "   sudo journalctl -u tendermonitor.service -f" -ForegroundColor Cyan
Write-Host ""

# Ask if user wants to connect to server now
# $response = Read-Host "Connect to server now? (y/n)"
# if ($response -eq "y" -or $response -eq "Y") {
#     Write-Host ""
#     Write-Host "Connecting to server $ServerHost..." -ForegroundColor Yellow
#     ssh $ServerHost
# }
