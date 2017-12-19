var topics = {};
var city_to_code = {'Moscow': 1, 'Saint-Petersburg': 2, 'Yaroslavl': 169};

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
    data.sort((a, b) => (b['score'] - a['score']));

    Plotly.purge('topics_wrapper');

    table = $('#search_table').DataTable();

    table.clear();
    topics = [];

    for(idx in data)
    {
        person = data[idx];

        topics[person['uid']] = person['topics'];

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
    var uid = $('#search_table').DataTable().row(idx).data()['Information']['uid'];
    topics_dist = topics[uid];
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
      "order": [[0, "desc"]],
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


function get_filters()
{
    var filters = {};

    var age_from = Math.max(18, Number($('#age_from').val()));
    var age_to = Math.max(99, Number($('#age_to').val()));

    var city = $('#city').val();

    if(city.length == 0)
    {
        city = 'Saint-Petersburg';
    }
    
    city = city_to_code[city];

    filters['city'] = city;
    filters['age_from'] = age_from;
    filters['age_to'] = age_to;
    filters['gender'] = 1;

    return filters;
}

$("#query_submit_btn").on("click", function () 
{
    var value = $('#query_text').val();
    
    if(value.length == 0)
    {
        alert("The query is empty!");
        return;
    }

    $("body").addClass("loading");   
    
    var filters = get_filters();
 
    $.ajax({
      type: 'GET',
      url: '/process_query',
      data: {'text': value, 'gender': filters['gender'], 'age_from':filters['age_from'], 'age_to':filters['age_to'],'city':filters['city'] },
      contentType: 'application/json',
      success: function(data)
      {
            if($("#search_div").is(':hidden'))
            {
                $("#search_div").show();
            }
            add_search_results(JSON.parse(data))
            $("body").removeClass("loading");
      },
      error: function(XMLHttpRequest, textStatus, errorThrown) 
      { 
        alert("Status: " + textStatus); alert("Error: " + errorThrown); 
      }  
    });
})
