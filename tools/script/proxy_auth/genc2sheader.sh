#!/bin/bash

# Set Proxy Creds

###### RSA and Yubi definitions
## proxy & port - The actual proxy used to access site
## validation_url - Any url available to the proxy. It is queried to validate the token.
##
rsa_proxy="c2sproxy.vip.ebay.com"
rsa_port="8080"
rsa_validation_url="https://c2sproxy.vip.ebay.com/wpad.dat"

yubi_proxy="c2syubi.vip.ebay.com"
yubi_port="8080"
yubi_validation_url="https://c2syubi.vip.ebay.com/wpadyubi.dat"

# The json config file. Not written by default.
jsonfile=~/.c2sconfig.json
writeconfig=0

# Default to Yubikey (RSA off)
use_rsa=0

showhelp() {
    echo "Synopsis: $0 [options]"
    echo "Options:"
    echo "  -h  --help         Show this help."
    echo "  -f  --file [FILE]  Write auth info to FILE. Default file: ${jsonfile}"
    echo "                     An existing file is moved to FILE.bak"
    echo "  -r  --rsa          Generate an rsa token. By default generates a yubikey token."
    echo ""
    echo "Generate a Yubikey or RSA login token that can be used by command line scripts"
    echo "to run commands over Corp-to-Site proxies. Using the -f option will write to a"
    echo "JSON file that can be used by programs that can read the file."
}

# Handle Cred Errors
errexit() {
  echo "Missing Credential"
  exit 2
}

# Set expires time
tmrdate() {
  OS_TYPE="`uname`"
  case $OS_TYPE in
	Darwin)
	   EXP_DATE="`date -v+1d`"
	   ;;
	Linux)
	   EXP_DATE="`date -d tomorrow`"
	   ;;
	*)
	   echo "Unknown platform"
	   exit 2
	   ;;
  esac
}

# Process command line args
while [[ $# -ge 1 ]]; do
    nextarg="$1"
    shift

    if [ "${nextarg}" = "-h" -o "${nextarg}" = "--help" ]; then
       showhelp
       exit 0
    elif [ "${nextarg}" = "-f" -o "${nextarg}" = "--file" ]; then
        writeconfig=1
        if [ $# -ge 1 ]; then
            if [[ "$1" != "-"* ]]; then
                jsonfile="$1"
                shift
             fi
        fi
    elif [ "${nextarg}" = "-r" -o "${nextarg}" = "--rsa" ]; then
        use_rsa=1
    else
        echo "Invalid argument: ${nextarg}"
        showhelp
        exit 1
    fi
done

proxy_host="${yubi_proxy}"
proxy_port="${yubi_port}"
validation_url="${yubi_validation_url}"

if [ "${use_rsa}" = "1" ]; then
    proxy_host="${rsa_proxy}"
    proxy_port="${rsa_port}"
    validation_url="${rsa_validation_url}"
fi

export http_proxy="http://${proxy_host}:${proxy_port}/"
export https_proxy="http://${proxy_host}:${proxy_port}/"

# Generate date
tmrdate

# Get Creds
echo -n "Enter username: "
read HTTP_USER
test -n "${HTTP_USER}" || errexit
echo -n "Enter token: "
read -s HTTP_TOKEN
test -n "${HTTP_TOKEN}" || errexit
echo ""

# Generate Header
auth=`echo -n ${HTTP_USER}:${HTTP_TOKEN} | base64`
CREATED_HEADER="Proxy-Authorization: Basic ${auth}"

# Do curl call to make token valid
curl -m 15 -o /dev/null -U "${HTTP_USER}:${HTTP_TOKEN}" "${validation_url}" &>/dev/null

# Output token if curl call succeeded
# Else exit w/ Authentication error
if [ "$?" = "0" ]; then
   echo "Token expires: ${EXP_DATE}"
   echo "${CREATED_HEADER}"
   if [ $writeconfig -gt 0 ]; then
      echo "Writing config file: ${jsonfile}"
      if [ -f $jsonfile ]; then
         mv $jsonfile "${jsonfile}.bak"
      fi
      echo "{ \"host\" : \"${proxy_host}\", \"port\" : \"${proxy_port}\", \"auth\" : \"${auth}\", \"expires\" : \"${EXP_DATE}\" }" > "${jsonfile}"
      if [ -f $jsonfile ]; then
         chmod u+rw,go-rw $jsonfile
      else
         echo "Failed to write config file"
      fi
   fi
   exit 0
else
   echo "Authentication Failure"
   exit 1
fi
