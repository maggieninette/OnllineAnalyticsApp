
function refreshTable() {
    
	
    $.ajax({
        type: 'GET',
        url: '/SalesAnalyticsController',
        success: function(response) {

            $.each(response, function(index, element) {
                $('.' + element).css('border', '3px solid #800080');
            });
        }
    });
	
	
    $.ajax({
        type: 'GET',
        url: '/CSE135_ShoppingApp/SalesAnalyticsController',
        success: function(response) {

            $.each(response, function(index, element) {
                $('.' + element).css('border', '3px solid #800080');
            });
        }
    });
}