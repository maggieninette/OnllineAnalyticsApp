
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
}