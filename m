#! /bin/bash


case "$1" in
    "install")
        echo "downloading"
        cd /tmp
        rm -rf /tmp/main.tar.gz
        wget -q https://github.com/cronusqiu90/ams/archive/refs/tags/main.tar.gz
        tar -xzvf /tmp/main.tar.gz -C /tmp

        echo "install"
        mkdir -p /home/auser/TRClient/AMS
        cd /home/auser/TRClient/AMS
        cp /tmp/ams-main/* .

        rm -rf /tmp/main.tar.gz
        rm -rf /tmp/ams-main
        
        echo "venv"
        rm -rf venv
        python -m venv venv
        venv/bin/pip install -q -U pip
        venv/bin/pip install -q -r requirements.txt

        echo "start"
        cp ams.ini /home/auser/supervisord.d/ams.ini
        supervisorctl update
        supervisorctl status IMAMS
        tail -n 5 run.log
        ;;
    "update")
        cd /tmp
        rm -rf /tmp/main.tar.gz
        wget -q https://github.com/cronusqiu90/ams/archive/refs/tags/main.tar.gz
        tar -xzvf /tmp/main.tar.gz -C /tmp
        mkdir -p /home/auser/TRClient/AMS
        cd /home/auser/TRClient/AMS
        cp /tmp/ams-main/* .
        rm -rf /tmp/main.tar.gz
        rm -rf /tmp/ams-main
        supervisorctl update
        ;;
esac
