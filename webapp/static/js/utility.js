var topics = [];

function add_search_results(data)
{
    table = $('#search_table').DataTable();
    for(idx in data)
    {
        person = data[idx];

        topics.push(person['topics']);

        table.row.add({'Name': {'text': person['first_name'] + ' ' + person['last_name'], 'id':person['uid']},
                    'Age':person['age'],
                    'City': person['city'],
                    'Sex':person['sex']}).draw()
    }    
}

function plot_topics(idx)
{
    topics_dist = topics[idx];
    topics_names = []

    for(tidx in topics_dist)
    {
        topics_names.push('Topic #' + tidx);
    }

    var data = [{
      type: 'bar',
      x: topics_dist,
      y: topics_names,
      orientation: 'h'
    }];

    Plotly.newPlot('topics_wrapper', data, {
        title: 'Topics',
        font: {
        size: 16
        },
            margin: {
                t: 30, //top margin
                l: 70, //left margin
                r: 20, //right margin
                b: 20 //bottom margin
            }
        });

    // Plotly.plot(gd, [{
    //     type: 'bar',
    //     x: [1, 2, 3, 4],
    //     y: [5, 10, 2, 8],
    //     marker: {
    //     color: '#C8A2C8',
    //     line: {
    //     width: 2.5
    //     }
    //     }
    //     }], {
    //     title: 'Auto-Resize',
    //     font: {
    //     size: 16
    //     },
    //         margin: {
    //             t: 20, //top margin
    //             l: 20, //left margin
    //             r: 20, //right margin
    //             b: 20 //bottom margin
    //         }
    //     });
}

$(document).ready(function() {
    $('#search_table').DataTable( {
        select: true,
        "searching": false,
        "columns": [
                      { 
                         "data": "Name",
                         "render": function(data, type, row, meta){
                            if(type === 'display'){
                                data = '<a href="https://vk.com/id' + data['uid'] + '">' + data['text'] + '</a>';
                            }
                            
                            return data;
                         }
                      },
                      { "data": "City" },
                      { "data": "Sex" }, 
                      { "data": "Age" },   

                   ]
    } );

    $('#search_table').on('click', 'tbody tr', function(event) {
        $(this).addClass('highlight').siblings().removeClass('highlight')
        plot_topics($(this).index());
    });
});

$("#query_submit_btn").on("click", function () 
{
    var value = $('#query_text').val();
    
    if(value.length == 0)
    {
        alert("The query is empty!");
        return;
    }

    $.ajax({
      type: 'GET',
      url: '/process_query',
      data: {'text': value, 'gender':1, 'city':2},
      contentType: 'application/json',
      success: function(data)
      {
            if($("#search_div").is(':hidden'))
            {
                $("#search_div").show();
            }
            add_search_results(JSON.parse(data))
            // console.log(JSON.parse(data))
      },
      error: function(XMLHttpRequest, textStatus, errorThrown) { 
        alert("Status: " + textStatus); alert("Error: " + errorThrown); 
      }  
    });
})
