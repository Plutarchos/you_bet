# Setup scheduled tasks for YouBet updates
$taskPath = "C:\Users\Emmet\you_bet\run_update.bat"

# Task for 00:01
$action1 = New-ScheduledTaskAction -Execute $taskPath
$trigger1 = New-ScheduledTaskTrigger -Daily -At "00:01"
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName "YouBet_Update_0001" -Action $action1 -Trigger $trigger1 -Settings $settings -Force
Write-Host "Created task: YouBet_Update_0001 (daily at 00:01)"

# Task for 10:00
$action2 = New-ScheduledTaskAction -Execute $taskPath
$trigger2 = New-ScheduledTaskTrigger -Daily -At "10:00"
Register-ScheduledTask -TaskName "YouBet_Update_1000" -Action $action2 -Trigger $trigger2 -Settings $settings -Force
Write-Host "Created task: YouBet_Update_1000 (daily at 10:00)"

Write-Host "`nScheduled tasks created successfully!"
Write-Host "View them with: Get-ScheduledTask -TaskName 'YouBet*'"
