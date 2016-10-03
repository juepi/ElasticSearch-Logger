########################### ElasticSearch-Logger #########################################
### Author: jpichlbauer
### Logs all environmental data from Sensors (MQTT Broker) to ElasticSearch via LogStash
##########################################################################################
# Use invariant Culture to avoid problems with comma seperator
[System.Threading.Thread]::CurrentThread.CurrentCulture = [Globalization.CultureInfo]::InvariantCulture;

# No console outputs in background mode
$BackgroundMode = $true

# common settings
$BaseDir = $PSScriptRoot
$LogFile = "$($BaseDir)\Logs\ElasticSearch-Logger.log"


#region MQTT Settings
$MQTT = [hashtable] @{}
$MQTT.MosqSub = "$($env:MOSQUITTO_DIR)\mosquitto_sub.exe"
$MQTT.MosqPub = "$($env:MOSQUITTO_DIR)\mosquitto_pub.exe"
$MQTT.Broker = 'dvb-juepi.mik'
$MQTT.Port = [int]'1883'
$MQTT.ClientID = 'ES-Logger'
$MQTT.StatusNewData = 'DataUpdated'
$MQTT.StatusOldData = 'DataObtained'
$MQTT.StatusSensorDead = 'SensorDead'
$MQTT.Topic_Test = 'HB7/ES-Logger/Test'
$MQTT.Topic_Out_Temp = 'HB7/Outdoor/Temp'
$MQTT.Topic_Out_RH = 'HB7/Outdoor/RH'
$MQTT.Topic_Out_Vbat = 'HB7/Outdoor/Vbat'
$MQTT.Topic_Out_AP = 'HB7/Outdoor/AirPress'
$MQTT.Topic_Out_Status = 'HB7/Outdoor/Status'
$MQTT.Topic_In_WZ_Temp = 'HB7/Indoor/WZ/Temp'
$MQTT.Topic_In_WZ_RH = 'HB7/Indoor/WZ/RH'
$MQTT.Topic_In_WZ_Vbat = 'HB7/Indoor/WZ/Vbat'
$MQTT.Topic_In_WZ_Sumo = 'HB7/Indoor/WZ/Sumo'
$MQTT.Topic_In_WZ_Status = 'HB7/Indoor/WZ/Status'

# FHEM Settings
$FHEM = [hashtable] @{}
$FHEM.Device = [hashtable] @{}
$FHEM.Reading = [hashtable] @{}
$FHEM.Device.HkOg1Gn = 'HK_OG1_GN_Clima'
$FHEM.Device.AD = 'ActionDetector'
$FHEM.Reading.HkRoomTemp = 'measured-temp'
$FHEM.Reading.AD_HkOg1Gn = 'status_HK_OG1_GN'
$FHEM.AD_Status_OK = 'alive'

# Hashtable for Logstash JSON output
$LS = [hashtable] @{}
$LS.HB7 = [hashtable] @{}
$LS.HB7.Outdoor = [hashtable] @{}
$LS.HB7.Indoor = [hashtable] @{}
$LS.HB7.Indoor.WZ = [hashtable] @{}
$LS.HB7.Indoor.OG1_Gn = [hashtable] @{}



#region Sensor Corrections
#Battery Voltage correction divider (/1000 -> sensor reports milliVolts) plus correction
[int]$VbatCorrDiv = 1000
#endregion

#region Mail Alerting
# Mail alerting
$MailSource="***"
$MailDest="***"
$MailSubject="ElasticSearch-Logger "
$MailText="ElasticSearch-Logger reports:`n`n"
$MailSrv="***"
$MailPort="25"
$MailPass = ConvertTo-SecureString "MailPasswordHere" -AsPlainText -Force
$MailCred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "MailUsernameHere",$MailPass
#endregion

# Ignore Sensor reading faults this number of times before throwing an exception (and mailing an error)
[Int]$MaxErrors = 4

# Helpers
[Int]$Script:SensOutErrCount = 0
[Int]$Script:SensWZErrCount = 0

# Load common Functions
Import-Module $BaseDir\Common-functions.ps1 -ErrorAction Stop


#region Local Functions
function write-log ([string]$message)
{
    write-output ((get-date).ToString() + ":: " + $message) | Out-File -append -filepath $LogFile
}
#endregion


################ Main ####################

if ($BackgroundMode)
{
    write-log -message "ElasticSearch-Logger: started in Background mode."
}
else
{
    write-log -message "ElasticSearch-Logger: ElasticSearch-Logger.ps1 script started in Foreground mode."
    Write-Host "ElasticSearch-Logger starting.." -ForegroundColor Green
}


# Start loop every 15 minutes
while(WaitUntilFull15Minutes)
{
    # Get Status of each Sensor
    try { [string]$SensOutStat = (Get-MqttTopic -Topic $MQTT.Topic_Out_Status) } catch { write-log -message ("Get-MqttTopic failed to fetch " + $MQTT.Topic_Out_Status + "; Exception: " + ($_.Exception.Message.ToString() -replace "`t|`n|`r"," ")) }
    try { [string]$SensWZStat = (Get-MqttTopic -Topic $MQTT.Topic_In_WZ_Status) } catch { write-log -message ("Get-MqttTopic failed to fetch " + $MQTT.Topic_In_WZ_Status + "; Exception: " + ($_.Exception.Message.ToString() -replace "`t|`n|`r"," ")) }
    try { [string]$HkOg1GnStat = (Get-Fhem-Readings -Device $FHEM.Device.AD -Reading $FHEM.Reading.AD_HkOg1Gn -Datatype string) } catch { write-log -message ("Get-Fhem-Readings failed to fetch data. Exception: " + ($_.Exception.Message.ToString() -replace "`t|`n|`r"," ")) }

    if ($SensOutStat -match $MQTT.StatusNewData)
    {
        # Reset Error counter
        $Script:SensOutErrCount = 0
        # Set Outdoor Sensor Status to data obtained
        Set-MqttTopic -Topic $MQTT.Topic_Out_Status -Value $MQTT.StatusOldData -Retain
    }
    else
    {
        # Increase Error Counter and Stop at Limit
        $Script:SensOutErrCount ++
        if ( $Script:SensOutErrCount -lt $MaxErrors )
        {
            write-log -message "Main: Errors triggered for OUTDOOR sensor! Errorcount: $($Script:SensOutErrCount), script will continue."
        }
        else
        {
            # OUTDOOR Sensor obviously broken
            Send-Email -Type ERROR -Message ("ElasticSearch-Logger reports OUTDOOR sensor offline!`nProgram will exit.") -Priority high | Out-Null
            write-log -message "Main: OUTDOOR Sensor error triggered $($MaxErrors) times, Script stopped!"
            # Update Status topic for sensor so other subscribers know that the sensor is dead
            Set-MqttTopic -Topic $MQTT.Topic_Out_Status -Value $MQTT.StatusSensorDead -Retain
            Write-Error "Main: OUTDOOR Sensor error triggered $($MaxErrors) times, Script stopped!" -ErrorAction Stop
        }
    }

    if ($SensWZStat -match $MQTT.StatusNewData)
    {
        # Reset Error counter
        $Script:SensWZErrCount = 0
        # Set WZ Sensor Status to data obtained
        Set-MqttTopic -Topic $MQTT.Topic_In_WZ_Status -Value $MQTT.StatusOldData -Retain
    }
    else
    {
        # Increase Error Counter and Stop at Limit
        $Script:SensWZErrCount ++
        if ( $Script:SensWZErrCount -lt $MaxErrors )
        {
            write-log -message "Main: Errors triggered for INDOOR-WZ sensor! Errorcount: $($Script:SensWZErrCount), script will continue."
        }
        else
        {
            # INDOOR-WZ Sensor obviously broken
            Send-Email -Type ERROR -Message ("ElasticSearch-Logger reports INDOOR-WZ sensor offline!`nProgram will exit.") -Priority high | Out-Null
            write-log -message "Main: INDOOR-WZ Sensor error triggered $($MaxErrors) times, Script stopped!"
            # Update Status topic for sensor so other subscribers know that the sensor is dead
            Set-MqttTopic -Topic $MQTT.Topic_In_WZ_Status -Value $MQTT.StatusSensorDead -Retain
            Write-Error "Main: INDOOR-WZ Sensor error triggered $($MaxErrors) times, Script stopped!" -ErrorAction Stop
        }
    }

    if ($HkOg1GnStat -match $FHEM.AD_Status_OK)
    {
        try
        {
            [single]$LS.HB7.Indoor.OG1_Gn.Temp = Get-Fhem-Readings -Device $FHEM.Device.HkOg1Gn -Reading $FHEM.Reading.HkRoomTemp -Datatype float
        }
        catch
        {
            write-log -message ("Get-Fhem-Readings failed to fetch temperature data. Exception: " + ($_.Exception.Message.ToString() -replace "`t|`n|`r"," "))
        }
    }


    # Push Sensor Data from MQTT Broker to ElasticSearch

    if (!$BackgroundMode) {Write-Host "Fetching Data from MQTT broker.." -ForegroundColor Green}
    # OUTDOOR Sensor
    try { [single]$LS.HB7.Outdoor.Temp = (Get-MqttTopic -Topic $MQTT.Topic_Out_Temp) } catch { write-log -message ("Get-MqttTopic failed to fetch " + $MQTT.Topic_Out_Temp + "; Exception: " + ($_.Exception.Message.ToString() -replace "`t|`n|`r"," ")) }
    try { [Single]$LS.HB7.Outdoor.Vbat = [math]::round(((Get-MqttTopic -Topic $MQTT.Topic_Out_Vbat) / $VbatCorrDiv),2) } catch { write-log -message ("Get-MqttTopic failed to fetch " + $MQTT.Topic_Out_Vbat + "; Exception: " + ($_.Exception.Message.ToString() -replace "`t|`n|`r"," ")) }
    try { [Single]$LS.HB7.Outdoor.RH = (Get-MqttTopic -Topic $MQTT.Topic_Out_RH) } catch { write-log -message ("Get-MqttTopic failed to fetch " + $MQTT.Topic_Out_RH + "; Exception: " + ($_.Exception.Message.ToString() -replace "`t|`n|`r"," ")) }
    # AirPressure needs to be converted from Pa -> mBar (/100)
    try { [Single]$LS.HB7.Outdoor.AirPress = ([single](Get-MqttTopic -Topic $MQTT.Topic_Out_AP) * 0.01) } catch { write-log -message ("Get-MqttTopic failed to fetch " + $MQTT.Topic_Out_AP + "; Exception: " + ($_.Exception.Message.ToString() -replace "`t|`n|`r"," ")) }

    # INDOOR-WZ Sensor
    try { [single]$LS.HB7.Indoor.WZ.Temp = (Get-MqttTopic -Topic $MQTT.Topic_In_WZ_Temp) } catch { write-log -message ("Get-MqttTopic failed to fetch " + $MQTT.Topic_In_WZ_Temp + "; Exception: " + ($_.Exception.Message.ToString() -replace "`t|`n|`r"," ")) }
    try { [Single]$LS.HB7.Indoor.WZ.Vbat = [math]::round(((Get-MqttTopic -Topic $MQTT.Topic_In_WZ_Vbat) / $VbatCorrDiv),2) } catch { write-log -message ("Get-MqttTopic failed to fetch " + $MQTT.Topic_In_WZ_Vbat + "; Exception: " + ($_.Exception.Message.ToString() -replace "`t|`n|`r"," ")) }
    try { [Single]$LS.HB7.Indoor.WZ.RH = (Get-MqttTopic -Topic $MQTT.Topic_In_WZ_RH) } catch { write-log -message ("Get-MqttTopic failed to fetch " + $MQTT.Topic_In_WZ_RH + "; Exception: " + ($_.Exception.Message.ToString() -replace "`t|`n|`r"," ")) }
    try { [Single]$LS.HB7.Indoor.WZ.Sumo = (Get-MqttTopic -Topic $MQTT.Topic_In_WZ_Sumo) } catch { write-log -message ("Get-MqttTopic failed to fetch " + $MQTT.Topic_In_WZ_Sumo + "; Exception: " + ($_.Exception.Message.ToString() -replace "`t|`n|`r"," ")) }

    # Send Data to ElasticSearch..
    if (!$BackgroundMode) {Write-Host "Push JSON data to LogStash.." -ForegroundColor Green}
    SendTo-LogStash -JsonString "$($LS | ConvertTo-Json -Compress -Depth 3)" | Out-Null
    if (! $BackgroundMode) { Write-Host "$($LS | ConvertTo-Json -Compress -Depth 3)" }
}
