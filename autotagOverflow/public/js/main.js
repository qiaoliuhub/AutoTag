$(function() {
  $("#my-form").submit(function(e){
      e.preventDefault();
  		var username = $('#username').val();
      var password = $('#password').val();
  		var data = { "username": username, "password": password };
  		$.ajax({
      		url: "/signin",
      		type: 'POST',
          data: data,
      		success:function(data,status){
              if(status == 'success'){
                location.href='/home';
              }
          },
          error:function(data,status,e){
              alert('username and password are not correct');
              if(status == "error"){
                 location.href='/signin';
               }
          }
  		});
	});

  $("#post-form").submit(function(e){
    e.preventDefault();
    var posts = $('#posts').val();
    var id = Math.floor(Math.random() * 2000000);
    // default admin user id
    var userid = 1;
    var data = {"posts":posts, "id": id, "userid": userid};
    $.ajax({
      url: "/home",
      type:'POST',
      data:data,
      success:function(data,status){
          if(status == 'success'){
            var newtitle="New Post";
            var newpost=data.post_position;
            var post_id=data.post_id;
            var newcontainer='<h3 id = "newtitle" class="form-signin-heading">'+newtitle+'</h3>\
            <p><pre id="newpost" class="form-control">'+newpost+'</pre></p>\
            <h3 class="form-signin-heading">Post ID</h3>\
            <p class="form-control">'+post_id+'</p>\
            <h3 class="form-signin-heading">Tag</h3>\
            <p id= predicted_tag class="form-control"></p>\
            <a href="/home" class="btn btn-lg btn-primary btn-block">Try More</a>\
            <script src="/js/get_tag.js"></script>';
            $('#newcontainer').html(newcontainer);
          }
      },
      error:function(data, status){
        if (status == 'error'){
          console.log('error');
          location.href='/home';
        }
      }
    });
    $.ajax({
      url:"http://localhost:5000/home",
      type:"POST",
      // contentType: "application/json",
      dataType:"json",
      data: JSON.stringify(data),
      success:function(data,status){
        if (status == 'success'){
          alert('sent request successfully')
        }
      }
    });
  })

});