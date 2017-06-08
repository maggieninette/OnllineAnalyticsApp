
function refreshTable() {

    $('#new-topk-products-outer').css('visibility', 'visible');
	
    $.ajax({
        type: 'GET',
        url: '/SalesAnalyticsController',
        dataType: 'text json',
        success: function(response) {
            var noLongerTopKProducts = $.parseJSON(response[0]);
            var newTopKProducts = $.parseJSON(response[1]);
            var updatedTotalSales = $.parseJSON(response[2]);

            $.each(noLongerTopKProducts, function(index, element) {
                $('.' + element).css('border', '3px solid #800080');
            });

            $.each(newTopKProducts, function(index, element) {
                var $this = $(this);
                if(index != newTopKProducts.length - 1) {
                    $('#new-topk-products-inner').append(element + ', ');
                }
                else
                    $('#new-topk-products-inner').append(element);
            });

            $.map(updatedTotalSales, function(index, element) {
                console.log(index);
                $('#' + element).html(index);
                $('#' + element).css('color', 'red');
            });
        }
    });

}