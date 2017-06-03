$(document).read(function(){
	$('#analyzeSales').submit(function(){
		
		$.ajax({
			url: 'SalesAnalyticsController',
			type: 'POST',
			dataType: 'json',
			data: $('#analyzeSales').serialize(),
			success: function(data){
					$('#displayAnalytics').html('test ajax');
					$('displayName').slideDown(500);

			}
		});
		return false;
	});
});