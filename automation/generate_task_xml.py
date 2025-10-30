import getpass
from pathlib import Path


def generate_task_xml():
    user = getpass.getuser()
    xml_template = f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Author>CryptoTaxCalc</Author>
    <Description>Nightly smoke test; Telegram alerts on failure + a success ping on start.</Description>
  </RegistrationInfo>

  <Triggers>
    <CalendarTrigger>
      <!-- Start at 2:15 AM daily -->
      <StartBoundary>2025-10-22T02:15:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByDay>
        <DaysInterval>1</DaysInterval>
      </ScheduleByDay>
    </CalendarTrigger>
  </Triggers>

  <Principals>
    <Principal id="Author">
      <UserId>{user}</UserId>
      <LogonType>Password</LogonType>
      <RunLevel>HighestAvailable</RunLevel>
    </Principal>
  </Principals>

  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>true</RunOnlyIfNetworkAvailable>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT20M</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>

  <Actions Context="Author">
    <Exec>
      <Command>C:\\Users\\{user}\\Desktop\\CryptoTaxCalc\\.venv\\Scripts\\python.exe</Command>
      <Arguments>"C:\\Users\\{user}\\Desktop\\CryptoTaxCalc\\automation\\run_smoke_and_email.py" --ping-start</Arguments>
      <WorkingDirectory>C:\\Users\\{user}\\Desktop\\CryptoTaxCalc\\automation</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"""
    out_path = Path(__file__).parent / "nightly_smoke_task.xml"
    out_path.write_text(xml_template, encoding="utf-16")
    print(f"✅ Generated XML for user {user}")
    print(f"📄 Saved to: {out_path}")


if __name__ == "__main__":
    generate_task_xml()
