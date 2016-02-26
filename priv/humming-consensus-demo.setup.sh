#!/bin/sh

echo "Step: Verify that the required entries in /etc/hosts are present"
for i in 1 2 3; do
    grep machi$i /etc/hosts | egrep -s '^127.0.0.1' > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo ""
        echo "'grep -s machi$i' failed. Aborting, sorry."
        exit 1
    fi
    ping -c 1 machi$i > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo ""
        echo "Ping attempt on host machi$i failed. Aborting."
        echo ""
        ping -c 1 machi$i
        exit 1
    fi
done

echo "Step: add a verbose logging option to app.config"
for i in 1 2 3; do
    ed ./dev/dev$i/etc/app.config <<EOF > /dev/null 2>&1
/verbose_confirm
a
{chain_manager_opts, [{private_write_verbose_confirm,true}]},
{stability_time, 1},
.
w
q
EOF
done

echo "Step: start three three Machi application instances"
for i in 1 2 3; do
    ./dev/dev$i/bin/machi start
    ./dev/dev$i/bin/machi ping
    if [ $? -ne 0 ]; then
        echo "Sorry, a 'ping' check for instance dev$i failed. Aborting."
        exit 1
    fi
done

echo "Step: configure one chain to start a Humming Consensus group with three members"

# Note: $CWD of each Machi proc is two levels below the source code root dir.
LIFECYCLE000=../../priv/quick-admin-examples/demo-000
for i in 3 2 1; do
    ./dev/dev$i/bin/machi-admin quick-admin-apply $LIFECYCLE000 machi$i
    if [ $? -ne 0 ]; then
        echo "Sorry, 'machi-admin quick-admin-apply failed' on machi$i. Aborting."
        exit 1
    fi
done

exit 0
