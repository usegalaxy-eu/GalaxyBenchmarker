#!/usr/bin/expect -f

set timeout -1

spawn iinit

expect "Enter your current iRODS password:"

send -- "adminpass\r"

expect eof
