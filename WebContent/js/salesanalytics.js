(function() {

})();

function refreshTable() {

    $.get('/SalesAnalyticsController', function(responseText) {
        if (responseText == 'sample response text')
            alert('AJAX succeeded.');
        else
            alert('AJAX failed.');
    });
}