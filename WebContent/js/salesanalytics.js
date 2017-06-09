
function refreshTable() {

    $.ajax({
        type: 'GET',
        url: '/SalesAnalyticsController',
        dataType: 'text json',
        success: function(response) {

            $('#new-topk-products-outer').css('visibility', 'visible');

            var noLongerTopKProducts = $.parseJSON(response[0]);
            var newTopKProducts = $.parseJSON(response[1]);
            var updatedTotalSales = $.parseJSON(response[2]);

            if (newTopKProducts.length == 0) {
                $('#new-topk-products-outer').html('<h3>No new products are included in the top 50.</h3>');
            }

            $.each(noLongerTopKProducts, function(index, element) {
                $('.' + element).css('border', '3px solid #800080');
            });

            $.each(newTopKProducts, function(index, element) {
                var $this = $(this);

                if (index != newTopKProducts.length - 1) {
                    $('#new-topk-products-inner').append(element + ', ');
                }
                else
                    $('#new-topk-products-inner').append(element);
            });

            $.each(updatedTotalSales, function(key, value) {
                $('#' + key).html(value);
                $('#' + key).css('color', 'red');
            });
        }
    });

}