#!/usr/bin/env node
/* 
 * nagios check for flume
 *
 * Authors: TJ Cox <surflure@gmail.com>
 *          Dave Eddy <dave@daveeddy.com>
 * Date: 10/18/14
 * License: to code
 *
 */


var fs = require('fs');
var http = require('http');

var box = process.argv[2] || fs.readFileSync('/somedir/internalip', 'utf-8').trim(),
    checktype = process.argv[3] || "stdout";

// options for GET
var optionsget = {
    host : box,
    port : 34545,
    path : '/metrics'
};

var warns = [];

var d = new Date();

var now_utc_ms = d.getTime() - (d.getTimezoneOffset() * 60000);



/* ================

     A range of checks for 
     various flume components.

   ================ */


function do_flume_check(fm, bucket) {

    //
    // is the channel getting full?
    //
    // * the 1000.0 (& 1.0) numbers below fix
    //   the current threshold at > 0.1 percent.
    //   a good starting point, but ...
    //
    if ((1000.0*parseFloat(fm["CHANNEL."+bucket].ChannelFillPercentage)) > 1.0)
        warns.push(bucket+" channel is "+(Math.round(100.0*parseFloat(fm["CHANNEL."+bucket].ChannelFillPercentage))/100.0)+" full");

    //
    // similar to the above ... but
    // calculate explicitly
    //
    if ((parseFloat(fm["CHANNEL."+bucket].EventPutSuccessCount)/parseFloat(fm["CHANNEL."+bucket].EventTakeSuccessCount)) > 2.0)
        warns.push(bucket+" has put/take > 2");

    //
    // the channel is full or mis-configured- I saw
    // this test on a Cloudera presentation by
    // Arvind Prabhakar + collaborators
    //
    if ((parseFloat(fm["CHANNEL."+bucket].EventPutAttemptCount)/parseFloat(fm["CHANNEL."+bucket].EventPutSuccessCount)) > 1.1)
        warns.push(bucket+" channel has put problems");

    //
    // the following is useless because take
    // attempts always increase ...
    //
    //if ((parseFloat(fm["CHANNEL."+bucket].EventTakeAttemptCount)/parseFloat(fm["CHANNEL."+bucket].EventTakeSuccessCount)) > 1.1)
    //    warns.push(bucket+" channel has take problems");

    //
    // failed sink connections outnumber created sink
    // connections - that doesn't sound good
    // a low value of "callTimeout" (~10's of seconds) will
    // trigger this .... a lot.
    //
    //if ((parseFloat(fm["SINK."+bucket].ConnectionFailedCount)/parseFloat(fm["SINK."+bucket].ConnectionCreatedCount)) > 2.0)
    //    warns.push(bucket+" failing to connect");

    //
    // test for successful event drain fraction
    //
    if ((parseFloat(fm["SINK."+bucket].EventDrainSuccessCount)/parseFloat(fm["SINK."+bucket].EventDrainAttemptCount)) < 0.99)
        warns.push(bucket+" not draining well");

    //
    // assess channel/sink connection
    //
    if (parseFloat(fm["CHANNEL."+bucket].EventTakeSuccessCount) != parseFloat(fm["SINK."+bucket].EventDrainSuccessCount))
        warns.push(bucket+" channel/sink discrepancy");

    //
    // channel or sink has been stopped ...
    //
    if (Math.round(fm["CHANNEL."+bucket].StopTime) > 0)
        warns.push(bucket+" channel has been stopped");

    if (Math.round(fm["SINK."+bucket].StopTime) > 0)
        warns.push(bucket+" sink has been stopped");

}


/* =================================

     Output general info to stdout

   ================================= */


function compute_ratio_rounded(numerator, denominator) {
    // this assumes that all denominators are integers
    if (Math.abs(denominator) < 1)
        return 0.0;

    return Math.round(100.0 * parseFloat(numerator) / parseFloat(denominator)) / 100.0;
}


function print_metric_info_header() {
    console.log("                      -------------------  CHANNEL  -------------------      ----------------------------  SINK  ------------------------------");
    console.log("bucket                capacity   size  fill%      takes put/take   put%      create (close/ fail)   events (success%) rate/s underflow    empty");
    console.log("------                -------------------------------------------------      ------------------------------------------------------------------");
    return;
}


function print_metric_info(fm, bucket) {

    // CHANNEL

    var chan_cap = Math.round(fm["CHANNEL."+bucket].ChannelCapacity);
    var chan_size = Math.round(fm["CHANNEL."+bucket].ChannelSize);
    var chan_uptime_ms = now_utc_ms - Math.round(fm["CHANNEL."+bucket].StartTime);
    var chan_downtime_ms = now_utc_ms - Math.round(fm["CHANNEL."+bucket].StopTime);

    var chan_put_attempt = Math.round(fm["CHANNEL."+bucket].EventPutAttemptCount);
    var chan_put_success = Math.round(fm["CHANNEL."+bucket].EventPutSuccessCount);
    var chan_put_success_ratio = compute_ratio_rounded(chan_put_success,chan_put_attempt);

    var chan_take_attempt = Math.round(fm["CHANNEL."+bucket].EventTakeAttemptCount);  // always increases
    var chan_take_success = Math.round(fm["CHANNEL."+bucket].EventTakeSuccessCount);

    // I don't think this is what I think this is ....
    var chan_event_rate = compute_ratio_rounded(chan_take_attempt, chan_uptime_ms / 1000.0);

    var chan_puttake_ratio = compute_ratio_rounded(chan_put_success, chan_take_success);

    var chan_fill_per = Math.round(100.0*parseFloat(fm["CHANNEL."+bucket].ChannelFillPercentage))/100.0;


    // SINK

    var sink_uptime_ms = now_utc_ms - Math.round(fm["SINK."+bucket].StartTime);
    var sink_downtime_ms = now_utc_ms - Math.round(fm["SINK."+bucket].StopTime);

    var sink_conn_create = Math.round(fm["SINK."+bucket].ConnectionCreatedCount);
    var sink_conn_fail = Math.round(fm["SINK."+bucket].ConnectionFailedCount);
    var sink_conn_close = Math.round(fm["SINK."+bucket].ConnectionClosedCount);

    var sink_conn_failcreate_ratio = compute_ratio_rounded(sink_conn_fail, sink_conn_create);
    var sink_conn_closecreate_ratio = compute_ratio_rounded(sink_conn_close, sink_conn_create);

    var sink_drain_attempt = Math.round(fm["SINK."+bucket].EventDrainAttemptCount);
    var sink_drain_success = Math.round(fm["SINK."+bucket].EventDrainSuccessCount);

    var sink_successevents_ratio = compute_ratio_rounded(sink_drain_success, sink_drain_attempt);

    // total events, to calculate average event rate
    var sink_event_rate = compute_ratio_rounded(sink_drain_success, sink_uptime_ms / 1000.0);

    var sink_batch_complete = Math.round(fm["SINK."+bucket].BatchCompleteCount);
    var sink_batch_underflow = Math.round(fm["SINK."+bucket].BatchUnderflowCount);
    var sink_batch_empty = Math.round(fm["SINK."+bucket].BatchEmptyCount);


    // Any comments
    var comment = "";
    if (Math.round(fm["CHANNEL."+bucket].StopTime) > Math.round(fm["CHANNEL."+bucket].StartTime))
        comment += "channel is down ";
    if (Math.round(fm["SINK."+bucket].StopTime) > Math.round(fm["SINK."+bucket].StartTime))
        comment += "sink is down ";


    // Output
    console.log("%s %s %s %s\% %s  %s\%  %s\%    %s (%s/%s)   %s (%s\%)  %s  %s %s %s",
                           (bucket+"                    ").substring(0,20),
                           ("          "+chan_cap).slice(-10),
                           ("          "+chan_size).slice(-5),
                           ("          "+chan_fill_per).slice(-5),
                           ("          "+chan_take_attempt).slice(-10),
                           ("          "+chan_puttake_ratio).slice(-5),
                           ("          "+chan_put_success_ratio).slice(-5),
                           ("          "+sink_conn_create).slice(-8),
                           ("          "+sink_conn_closecreate_ratio).slice(-5),
                           ("          "+sink_conn_failcreate_ratio).slice(-5),
                           ("          "+sink_drain_success).slice(-8),
                           ("          "+sink_successevents_ratio).slice(-5),
                           ("          "+sink_event_rate).slice(-5),
                           ("          "+sink_batch_underflow).slice(-8),
                           ("          "+sink_batch_empty).slice(-8),
                           comment);

    return;
}


function print_raw_info(fm, bucket) {
    console.log("=====================");
    console.log("  bucket= "+bucket);
    console.log(fm["CHANNEL."+bucket]);
    console.log(fm["SINK."+bucket]);
}



/* ====================================

     Get the metrics and process ....

   ==================================== */


function process_flume_metrics(fm, bucket) {

    if (checktype == "stdout")
        print_metric_info_header();

    // get a distinct set of channel/sink combos and process
    var distinct_sinks = [];
    Object.keys(fm).forEach(function (key) {
        var bucket = key.split(".")[1];
        //console.log("key= "+key+"   "+bucket+"    "+distinct_sinks.indexOf(bucket));
        if((distinct_sinks.indexOf(bucket) < 0) && bucket != 'http' && bucket != "catchallmem") {
            distinct_sinks.push(bucket);
            switch (checktype) {
                case "raw":
                    print_raw_info(fm,bucket);
                    break;
                case "nagios":
                    try {
                        do_flume_check(fm,bucket);
                    } catch(e) {
                        console.log('unknown: failed to extract data - %s', e.message);
                        process.exit(3);
                    }
                    break;
                default:
                    print_metric_info(fm,bucket);
            }
        }
    });

    if (checktype == "nagios")
        if (warns.length > 0) {
            console.log('warning: %s', warns.join(','));
            process.exit(1);
        } else {
            console.log('ok: flume is fine');
            process.exit(0);
        }

}


// do the GET request
var reqGet = http.get(optionsget, function(res) {
    //console.log("statusCode: ", res.statusCode);
    //console.log("headers: ", res.headers);

    var flume_metrics = '';

    res.on('data', function(thischunk) {
        flume_metrics += thischunk;
    });

    res.on('end', function () {
        if (checktype != "nagios")
            console.log("\nFlume status for box= "+optionsget.host+"\n");
        var data;
        try {
            data = JSON.parse(flume_metrics);
        } catch (e) {
            console.log('unknown: failed to parse data - %s', e.message);
            process.exit(3);
        }
        process_flume_metrics(data);
    });

});

//reqGet.end();
reqGet.on('error', function(e) {
    if (checktype == "nagios") {
      console.log('critical: request failed - %s', e.message);
      process.exit(2);
    }
    console.error(e);
});
