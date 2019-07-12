@REM https://github.com/xerial/sbt-pack/blob/master/src/main/templates/launch-bat.mustache
@REM would be worth getting more inspiration from

@echo off

SET ERROR_CODE=0

SET LAUNCHER_PATH=%localappdata%/coursier-launcher

IF NOT EXIST "%LAUNCHER_PATH%" (
  bitsadmin /transfer "DownloadCoursierLauncher" https://github.com/coursier/coursier/raw/master/coursier "%LAUNCHER_PATH%"
)

SET CMD_LINE_ARGS=%*

@REM this is the hackiest thing ever. don't delete the extra newline here!
set NL=^


java -Xms512m -Xmx4g -Xss2m -XX:ReservedCodeCacheSize=512m -Dfile.encoding=UTF8 -Djna.nosys=true -Dline.separator=^%NL%%NL% -jar "%LAUNCHER_PATH%" launch org.scala-sbt:sbt-launch:1.0.2 -- %CMD_LINE_ARGS%

IF ERRORLEVEL 1 GOTO error
GOTO end

:error
SET ERROR_CODE=1

:end
SET LAUNCHER_PATH=
SET CMD_LINE_ARGS=

EXIT /B %ERROR_CODE%

