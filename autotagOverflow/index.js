//Using nodejs to build a web server

var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var multer = require('multer');

var server = require('http').createServer(app);
var path = require('path');
var cassandra = require('cassandra-driver');
var config = require('config');
var session = require('express-session');
var redis = require('redis');

//Set up static directiory
app.use(express.static(path.join(__dirname, 'public')));

// set ups session
app.use(session({
    secret:'secret',
    resave:true,
    saveUninitialized:false,
    cookie:{
        maxAge:1000*60*10  //expire time
    }
}));

//middleware to set up user information
app.use(function(req, res, next){
    res.locals.user = req.session.user;
    var err = req.session.error;
    res.locals.message = '';
    if (err) res.locals.message = '<div style="margin-bottom: 20px;color:red;">' + err + '</div>';
    next();
});

// Set up views and load views engine
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'html');
app.engine('.html', require('ejs').__express );
//Add bodyparser middleware and parse application body
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

//Set up cassandra client
var contactPointIP = config.get('Cassandra.contactPoints');
var keySpace = config.get('Cassandra.keyspace');
var table = config.get('Cassandra.table');
var cassandraClient = new cassandra.Client({contactPoints: [contactPointIP], keyspace: keySpace});

//Set up redis client
var redis_host = config.get('Redis.host');
var redis_port = config.get('Redis.port');
var redis_client = redis.createClient(redis_port, redis_host);

//Set up route 
app.get('/logout', function(req, res){
    req.session.user = null;
    req.session.error = null;
    res.redirect('signin');
});

app.get('/signin', function(req, res){
	res.render('signin');
})

app.get('/', function(req, res){
	res.render('signin');
})

app.get('/home', function(req, res){
	if(req.session.user){
        res.render('home');
    }else{
        req.session.error = "please log in first"
        res.redirect('signin');
    }
})

app.get('/home/:post_id', function(req, res){

	// get data from cassandra or redis based on id 
	var post_id = req.params.post_id;
	var tag;
  console.log('Retriving data for post_id %s', post_id);
	// cassandraClient.execute("SELECT tag FROM "+table +" WHERE id = '" + post_id + "'", function(err, result){
	// 	if (!err){
	// 		if ( result.rows.length > 0 ) {
 //               var user = result.rows[0];
 //               var tag=user.tag;
 //               res.send({tag: tag});
 //           } else {
 //               console.log("No results");
 //           }
	// 	}
	// });
  redis_client.hmget(post_id, 'tags', function(err, result){
    if (!err){
      if ( result.length > 0 ) {
           var tag = result[0];
           res.send({tag: tag});
       } else {
           console.log("No results");
       }
    }
  });
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
   		req.session.user = user;
   		res.sendStatus(200);
	}else{
		req.session.error = "username and password are not correct";
   		res.sendStatus(404);
	}
});



//server listen on port 5000
server.listen(3000, function(){
	console.log('server started on port 3000');
});