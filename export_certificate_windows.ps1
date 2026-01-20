# PowerShell скрипт для экспорта сертификата из Windows реестра
# Запустите этот скрипт на Windows машине с установленным сертификатом

Write-Host "Экспорт сертификата из Windows реестра..." -ForegroundColor Green

# Путь для сохранения сертификата
$certPath = "$PSScriptRoot\certificate.cer"

# Получаем все сертификаты из хранилища "Личное"
$certStore = New-Object System.Security.Cryptography.X509Certificates.X509Store("My", "CurrentUser")
$certStore.Open("ReadOnly")

Write-Host "`nНайденные сертификаты в хранилище 'Личное':" -ForegroundColor Yellow
$certs = $certStore.Certificates
for ($i = 0; $i -lt $certs.Count; $i++) {
    $cert = $certs[$i]
    Write-Host "[$i] $($cert.Subject) (Действителен до: $($cert.NotAfter))" -ForegroundColor Cyan
}

if ($certs.Count -eq 0) {
    Write-Host "`nОшибка: Сертификаты не найдены в хранилище 'Личное'!" -ForegroundColor Red
    $certStore.Close()
    exit 1
}

# Если несколько сертификатов, запрашиваем выбор
if ($certs.Count -gt 1) {
    $selection = Read-Host "`nВведите номер сертификата для экспорта (0-$($certs.Count-1))"
    try {
        $selectedIndex = [int]$selection
        if ($selectedIndex -lt 0 -or $selectedIndex -ge $certs.Count) {
            Write-Host "Неверный номер сертификата!" -ForegroundColor Red
            $certStore.Close()
            exit 1
        }
        $selectedCert = $certs[$selectedIndex]
    } catch {
        Write-Host "Ошибка: введите число!" -ForegroundColor Red
        $certStore.Close()
        exit 1
    }
} else {
    $selectedCert = $certs[0]
}

# Экспортируем сертификат в формате DER (бинарный)
try {
    $certBytes = $selectedCert.Export([System.Security.Cryptography.X509Certificates.X509ContentType]::Cert)
    [System.IO.File]::WriteAllBytes($certPath, $certBytes)
    Write-Host "`n✅ Сертификат успешно экспортирован в: $certPath" -ForegroundColor Green
    Write-Host "   Субъект: $($selectedCert.Subject)" -ForegroundColor Cyan
    Write-Host "   Действителен до: $($selectedCert.NotAfter)" -ForegroundColor Cyan
} catch {
    Write-Host "`n❌ Ошибка при экспорте сертификата: $_" -ForegroundColor Red
    $certStore.Close()
    exit 1
}

$certStore.Close()

Write-Host "`n📋 Следующие шаги:" -ForegroundColor Yellow
Write-Host "1. Скопируйте файл certificate.cer на Linux сервер" -ForegroundColor White
Write-Host "2. Разместите его в /etc/opt/cprocsр/stunnel/1.cer" -ForegroundColor White
Write-Host "3. Убедитесь, что закрытый ключ доступен на Linux сервере" -ForegroundColor White
Write-Host "4. Настройте stunnel.conf согласно инструкции" -ForegroundColor White

