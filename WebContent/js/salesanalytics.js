
function refreshTable() {

    $.ajax({
        type: 'GET',
        url: '/SalesAnalyticsController',
        dataType: 'text json',
        success: function(response) {

            $('#new-topk-products-outer').css('visibility', 'visible');

            if (jQuery.hasData(response[1])) {
                $('#new-topk-products-outer').html('<h3>The following products are now included in the top 50 products:</h3>')
            }

            var noLongerTopKProducts = $.parseJSON(response[0]);
            var newTopKProducts = $.parseJSON(response[1]);
            var updatedTotalSales = $.parseJSON(response[2]);

            $.each(noLongerTopKProducts, function(index, element) {
                $('.' + element).css('border', '3px solid #800080');
            });

            $.each(newTopKProducts, function(key, value) {
                $('#new-topk-products-inner').append(key + ' (' + value + ') ');
            });

            $.each(updatedTotalSales, function(key, value) {
                $('#' + key).html(value);
                $('#' + key).css('color', 'red');
            });
        }
    });

}