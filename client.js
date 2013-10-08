
var fluffyWsTest = {

    init: function() {

        fluffyWsTest.ws = new WebSocket( "ws://localhost:8888/sub/{{user}}" );

        fluffyWsTest.ws.onopen = function() {
            $('#msgValue').text( "Connected" );
        };

        fluffyWsTest.ws.onerror = function(e) {
            $('#msgValue').text( "ERROR" );
        };

        fluffyWsTest.ws.onclose = function(e) {
            $('#msgValue').text( "Closed" );
        };

        fluffyWsTest.ws.onmessage = function (e) {
            var obj = $.parseJSON( e.data );
            $('#msgValue').text( obj.body );
            fluffyWsTest.ws.send('{ "ack":'+obj.ackTag+' }' ); // send the ack to the message 
        };  

        $('#msgForm').submit(function(event) { 
            
            $.ajax({ 
                url   : "http://localhost:8888/pub/{{user}}",
                type  : "post",
                data  : $('#msg').val(), // data to be submitted
                success: function(response){
                   // TODO 
                }
            });

            return false; // prevent normal handling 
        });

        fluffyWsTest.doKeepAlice = function() {
            fluffyWsTest.ws.send('{ "ack": 0 }' ); // send the keepalive
            setTimeout( fluffyWsTest.doKeepAlice, 5*60*1000); // TODO jitter 
        };

        setTimeout( fluffyWsTest.doKeepAlice, 5000); // TODO jitter 
    }
};

$( document ).ready( fluffyWsTest.init );
