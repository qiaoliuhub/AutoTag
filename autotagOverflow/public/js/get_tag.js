$(function(){
	function get_tag(newpost_id){
	    var predicted_tag = $('#predicted_tag').val();
	    if (predicted_tag == ''){
	    	var data = {id: newpost_id};
	    	$.ajax({
	      		url: "/home/"+newpost_id,
	      		type: 'GET',
	      		success:function(data,status){
	              if(status == 'success'){
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
  	var get_tag_func = setInterval(function(){get_tag(newpost_id)}, 100);
});