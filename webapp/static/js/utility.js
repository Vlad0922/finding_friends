var topics = [];



if (!String.format) {
  String.format = function(format) {
    var args = Array.prototype.slice.call(arguments, 1);
    return format.replace(/{(\d+)}/g, function(match, number) { 
      return typeof args[number] != 'undefined'
        ? args[number] 
        : match
      ;
    });
  };
}

function add_search_results(data)
{
    Plotly.purge('topics_wrapper');

    table = $('#search_table').DataTable();

    table.clear();
    topics = [];

    for(idx in data)
    {
        person = data[idx];

        topics.push(person['topics']);

        table.row.add({
                        'Information': {
                                        'text': person['first_name'] + ' ' + person['last_name'], 
                                        'uid':person['uid'], 
                                        'city':person['city'], 
                                        'age':person['age'], 
                                        'sex':person['sex']
                                        },                        
                        'Photo': {'photo':person['photo']},
                        'Score': person['score'], 
                    }).draw()
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

    var layout = {
                    title: 'Topics',
                    font: { size: 16 },
                    margin: {
                        t: 30, //top margin
                        l: 70, //left margin
                        r: 20, //right margin
                        b: 20 //bottom margin
                    }  
                  }

    Plotly.newPlot('topics_wrapper', data, layout,  {staticPlot:true});
}

$(document).ready(function() {
  $('#search_table').DataTable( {
      select: true,
      "searching": false,
      "bInfo": false,
      "lengthChange": false,
      "columns": [
                    {"data" : "Score"},
                    {
                        "data" : "Photo",
                        "render": function(data, type, row, meta){
                            return '<img height="100px" src="'+ data['photo'] +'"/>';
                        }
                    },
                    { 
                       "data": "Information",
                       "render": function(data, type, row, meta)
                       {                            
                            txt = '<a href="https://vk.com/id' + data['uid'] + '">' + data['text'] + '</a>\n';
                            txt += String.format('Gender: {0}\n', data['sex']);
                            txt += String.format('City: {0}\n', data['city']);
                            txt += String.format('Age: {0}\n', data['age']);
                      
                            return txt;
                       }
                    },
                    { 
                        "data": "Feedback",
                        "render": function(data, type, row, meta){
                            return '<img height="25px" src="static/smiles_2.png"/>';
                        }}
                 ]
  });

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
      },
      error: function(XMLHttpRequest, textStatus, errorThrown) 
      { 
        alert("Status: " + textStatus); alert("Error: " + errorThrown); 
      }  
    });
})
