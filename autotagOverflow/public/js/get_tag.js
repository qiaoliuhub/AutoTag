$(function(){
	function get_tag(newpost_id){
	    var predicted_tag = $('#predicted_tag').val();
	    if (predicted_tag == ''){
	    	var data = {id: newpost_id};
	    	var start_time = new Date().getTime();
	    	$.ajax({
	      		url: "/home/"+newpost_id,
	      		type: 'GET',
	      		success:function(data,status){
	              if(status == 'success'){
	              	var after_time = new Date().getTime();
	              	// alert('It takes '+(after_time - start_time) + 'ms to get the tag!')
	              	var tag = data.tag;
	                $('#predicted_tag').text(tag);
		      		$('#predicted_tag').val(tag);
	              }
	            },
	            error:function(data,status,e){
	              if(status == "error"){
	                 location.href='/home';
	               }
	          	}
      	    });
	    }
	    else{
	      clearInterval(get_tag_func);
	    }
  	}
  	var newpost_id = $('#newpost_id').val();
  	var get_tag_func = setInterval(function(){get_tag(newpost_id)}, 1000);
});