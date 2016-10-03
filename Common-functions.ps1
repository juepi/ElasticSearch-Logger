########## Common Functions for ElasticSearch-Logger #############
######## requires MQTT Broker and SMTP config from main script! #####


function Get-MqttTopic ([String]$Topic)
{
    $pinfo = New-Object System.Diagnostics.ProcessStartInfo
    $pinfo.FileName = $MQTT.MosqPub
    $pinfo.RedirectStandardError = $true
    $pinfo.RedirectStandardOutput = $true
    $pinfo.UseShellExecute = $false
    $pinfo.Arguments = ("-h $($MQTT.Broker) -p $($MQTT.Port) -i $($MQTT.ClientID) -t $($MQTT.Topic_Test) -m ok -r")
    $pubproc = New-Object System.Diagnostics.Process
    $pubproc.StartInfo = $pinfo
    # Make sure broker is reachable by publishing to test topic
    $pubproc.Start() | Out-Null
    $pubproc.WaitForExit()
    if ( $pubproc.ExitCode -ne 0 )
    {
        # Broker not reachable
        return "ErrNoBroker"
    }
    # Subscribe to MQTT Topic and get most recent (retained) value
    $pinfo.FileName = $MQTT.MosqSub
    $pinfo.Arguments = ("-h $($MQTT.Broker) -p $($MQTT.Port) -i $($MQTT.ClientID) -t $($Topic) -C 1")
    $subproc = New-Object System.Diagnostics.Process
    $subproc.StartInfo = $pinfo
    $subproc.Start() | Out-Null
    # wait 1 sec for a result
    if ( ! $subproc.WaitForExit(1000) ) 
    {
        # topic / value probably doesn't exit
        try { $subproc.kill() } catch {}
        return "nan"
    }
    return ($subproc.StandardOutput.ReadToEnd()).Trim()
}


function Set-MqttTopic ([String]$Topic,[String]$Value,[switch]$Retain)
{
    $pinfo = New-Object System.Diagnostics.ProcessStartInfo
    $pinfo.FileName = $MQTT.MosqPub
    $pinfo.RedirectStandardError = $true
    $pinfo.RedirectStandardOutput = $true
    $pinfo.UseShellExecute = $false
    $pinfo.Arguments = ("-h $($MQTT.Broker) -p $($MQTT.Port) -i $($MQTT.ClientID) -t $($MQTT.Topic_Test) -m ok -r")
    $pubproc = New-Object System.Diagnostics.Process
    $pubproc.StartInfo = $pinfo
    # Make sure broker is reachable by publishing to test topic
    $pubproc.Start() | Out-Null
    $pubproc.WaitForExit()
    if ( $pubproc.ExitCode -ne 0 )
    {
        # Broker not reachable
        return $false
    }

    # Publish to MQTT Topic
    if ($Retain)
    {
        $pinfo.Arguments = ("-h $($MQTT.Broker) -p $($MQTT.Port) -i $($MQTT.ClientID) -t $($Topic) -m `"$($Value.ToString())`" -r")
    }
    else
    {
        $pinfo.Arguments = ("-h $($MQTT.Broker) -p $($MQTT.Port) -i $($MQTT.ClientID) -t $($Topic) -m `"$($Value.ToString())`"")
    }
    $pubproc.StartInfo = $pinfo
    $pubproc.Start() | Out-Null
    $pubproc.WaitForExit()    
    if ( $pubproc.ExitCode -ne 0 )
    {
        # Broker not reachable
        return $false
    }
    return $true
}


function Send-Email ([String]$Type,[String]$Message,[String]$Priority='normal')
{
    $MailSubject = ($Mailsubject + $Type)
    try
    {
        Send-MailMessage -To $MailDest -From $MailSource -Subject $MailSubject -Body ($MailText + $Message) -Priority $Priority -SmtpServer $MailSrv -Encoding ([System.Text.Encoding]::UTF8) -Credential $MailCred
    }
    catch
    {
        return $false
    }
    return $true
}


Function SendTo-LogStash ([string]$JsonString)
{ 
    if ($JsonString)
    {
        try
        {
            # Connect to local LogStash Service on TCP Port 5544 and send JSON string
            $Socket = New-Object System.Net.Sockets.TCPClient(127.0.0.1,5544)
            $Stream = $Socket.GetStream()
            $Writer = New-Object System.IO.StreamWriter($Stream)
            $Writer.WriteLine($JsonString)
            $Writer.Flush()
            $Stream.Close()
            $Socket.Close()
        }
        catch
        {
            return $false
        }
    }
    else
    {
        # No String parameter given
        return $false
    }
    return $true
}


function WaitUntilFull15Minutes ()
{
    # Function will sleep until the next full 15 minutes (00:00,00:15,00:30,...)
    $gt = Get-Date -Second 0
    do {Start-Sleep -Seconds 1} until ((Get-Date) -ge ($gt.addminutes(15-($gt.minute % 15))))
    return $true
}


Function Get-Fhem-Readings
{   Param (
        [Parameter(ValueFromPipeline=$true)]
        [String[]]$Device = (throw "No Device given!"),
        [String[]]$Reading = (throw "No reading given!"),
        [String[]]$Datatype = "string",
        [string]$TelnetHost = "localhost",
        [string]$Port = "7072",
        [int]$WaitTime = 20
    )
    #Connect to FHEM telnet port and acquire data
    try
    {
        $Socket = New-Object System.Net.Sockets.TcpClient($TelnetHost, $Port)
        $Stream = $Socket.GetStream()
        $Writer = New-Object System.IO.StreamWriter($Stream)
        $Buffer = New-Object System.Byte[] 1024 
        $Encoding = New-Object System.Text.AsciiEncoding

        #Build String to get desired value
        [string]$Command = '{ReadingsVal ("' + $Device + '", "' + $Reading + '", "")}'

        #Issue command
        $Writer.WriteLine($Command) 
        $Writer.Flush()
        Start-Sleep -Milliseconds $WaitTime

        $Result = ""
        #Save all the results
        While($Stream.DataAvailable) 
        {   $Read = $Stream.Read($Buffer, 0, 1024) 
            $Result += ($Encoding.GetString($Buffer, 0, $Read))
        }
    }
    Catch     
    {   
        Write-Error "Unable to connect to host: $($TelnetHost):$Port" -ErrorAction stop
    }

    # return the results
    switch ($Datatype)
    {
        string {return [string]$Result}
        float {return [float]$Result}
    }
}