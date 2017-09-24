var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var multer = require('multer');

var server = require('http').createServer(app);
var path = require('path');
var cassandra = require('cassandra-driver');
// var config = require('config');

//Set up static directiory
app.use(express.static(path.join(__dirname, 'public')));

// Set up views and load views engine
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'html');
app.engine('.html', require('ejs').__express );
//Add bodyparser middleware and parse application body
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

//Set up cassandra client
// var contactPointIP = config.get('Cassandra.contactPoints');
// var keySpace = config.get('Cassandra.keyspace');
// var table = config.get('Cassandra.table');
// var cassandraClient = new cassandra.Client({contactPoints: [contactPointIP], keyspace: keySpace});

//Set up route 
app.get('/signin', function(req, res){
	res.render('signin');
})

app.get('/', function(req, res){
	res.render('signin');
})

app.get('/home', function(req, res){
	if (req.body.id){
		console.log('have somthing');
		//get data from cassandra based on id
		var post_id = req.body.id;
		var tag;
		// cassandraClient.execute("SELECT tag FROM "+table +" WHERE id = " + post_id, function(err, result){
		// 	if (!err){
		// 		if ( result.rows.length > 0 ) {
  //                  var user = result.rows[0];
  //                  var tag=user.tag;
  //                  console.log("tag = %s", user.tag);
  //              } else {
  //                  console.log("No results");
  //              }
		// 	}
		// })
		
		//Check if the returned id is correct
		res.send({tag: tag});
	}
	else{
		console.log('have nothing');
		res.render('home');
	}
})

app.post('/home', function(req, res){
	var post_text=req.body.posts;
	var post_id=req.body.id;
	res.send({post_position:post_text, post_id:post_id});
})

app.get('/result', function(req, res){
	res.render('result');
})

app.post('/signin',function(req, res){//localhost:5000/signin
    var user={
        username:'admin',
        password:'admin'
    }
    console.log(req.body.username+' '+ req.body.password);
	if(req.body.username==user.username&&req.body.password==user.password)
	{ 
   		res.sendStatus(200);
	}else{
   		res.sendStatus(404);
	}
});



//server listen on port 5000
server.listen(3000, function(){
	console.log('server started on port 3000');
});