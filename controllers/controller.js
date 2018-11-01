const pg = require('pg')
const config  = require('../config').database_connection;


exports.times = function(req, res){
    var month, group, year, ips, operation;
    var queryOperation = "";
    var group_by = "";
    var where = "";
    //console.log(JSON.stringify(req.query));
    if(req.query.ips !=null){
        ips = req.query.ips;
    }else{
        ips = null;
    }

    if(req.query.month !=null){
        month = req.query.month
    }else{
        month = null
    }
    if(req.query.year !=null){
        year = req.query.year;
    }else{
        null;
    }

    if(req.query.group !=null){
        group = req.query.group;
    }else{
        group = 'year';
    }

    if(req.query.operation !=null){
        operation = req.query.operation;
    }else{
        operation = 'avg';
    }

    if(year!= null && month !=null){
        where+=" where year=" +year + " and month=" + month; 
        if(ips!=null){
            where += " and key_ips="+ips
        }
    }else if(year !=null && month == null){
        where+= " where year=" +year;
        if(ips!=null){
            where += " and key_ips="+ips
        }
    }else if(year == null && month !=null){
        where+= " where month=" + month; 
        if(ips!=null){
            where += " and key_ips="+ips
        }
    }else{
        if(ips!=null){
            where += " where key_ips="+ips
        }
    }
    
    if(operation =='avg'){
        queryOperation = 'avg(cm.tiempo_espera_horas) as op';
    }else{
        queryOperation = 'sum(cm.tiempo_espera_horas) as op';
    }
    var query = "select ";
    if(group == 'year'){
       group_by = "GROUP BY year";
       query += queryOperation+ ", dimf.year as label from citas_medicas cm join dim_fecha dimf ";
       query +="on cm.key_fecha_atencion = dimf.key_date ";
       query += where + " "  + group_by +  " order by label";
    }else if(group == 'ips'){
        group_by = "GROUP BY dimips.nombre";
        
        query+= queryOperation + ", dimips.nombre as label from citas_medicas cm join dim_fecha dimf ";
        query += " on cm.key_fecha_atencion = dimf.key_date join dim_ips dimips on dimips.key_ips = cm.key_ips ";
        query +=  where + " "+group_by+ " order by label";
    }else if(group == 'month'){
        group_by = "GROUP BY (month_long_label_eng, dimf.month)";
        query += queryOperation+ ", dimf.month_long_label_eng as label from citas_medicas cm join dim_fecha dimf ";
        query +="on cm.key_fecha_atencion = dimf.key_date ";
        query += where + " "  + group_by +  " order by dimf.month";
    }else if(group == 'day'){
        group_by = "GROUP BY day_of_week_long_label_eng";
        query += queryOperation+ ", dimf.day_of_week_long_label_eng as label from citas_medicas cm join dim_fecha dimf ";
        query +="on cm.key_fecha_atencion = dimf.key_date ";
        query += where + " "  + group_by +  " order by label";
    }
    console.log(query);
    const client = new pg.Client(config);
    client.connect();

    client.query(query)
        .then(function(result){
            client.end();

            return res.status(200).json(result.rows);
        })
        .catch(function(err){
            client.end();

            return res.status(500).json({message: err});
        })
}
exports.prices= function(req, res){
    var month, group, year, medicamento, operation;
    var queryOperation = "";
    var group_by = "";
    var where = "";



    if(req.query.month !=null){
        month = req.query.month
    }else{
        month = null
    }
    if(req.query.year !=null){
        year = req.query.year;
    }else{
        null;
    }

    if(req.query.group !=null){
        group = req.query.group;
    }else{
        group = 'year';
    }

    if(req.query.operation !=null){
        operation = req.query.operation;
    }else{
        operation = 'avg';
    }

    if(year!= null && month !=null){
        where+=" where year=" +year + " and month=" + month; 
        if(medicamento!=null){
            where += " and key_medicamento="+medicamento
        }
    }else if(year !=null && month == null){
        where+= " where year=" +year;
        if(medicamento!=null){
            where += " and key_medicamento="+medicamento
        }
    }else if(year == null && month !=null){
        where+= " where month=" + month; 
        if(medicamento!=null){
            where += " and key_medicamento="+medicamento
        }
    }else{
        if(medicamento!=null){
            where += " where key_medicamento="+medicamento
        }
    }

    if(operation =='avg'){
        queryOperation = 'avg(fm.precio_total) as op';
    }else{
        queryOperation = 'sum(fm.precio_total) as op';
    }
    var query = "select ";
    if(group == 'year'){
        group_by = "GROUP BY year";
        query += queryOperation+ ", dimf.year as label from formula_medica fm join dim_fecha dimf ";
        query +="on fm.key_date = dimf.key_date ";
        query += where + " "  + group_by +  " order by label";
     }else if(group == 'month'){
         group_by = "GROUP BY (month_long_label_eng, dimf.month)";
         query += queryOperation+ ", dimf.month_long_label_eng as label from  formula_medica fm join dim_fecha dimf ";
         query +="on fm.key_date = dimf.key_date ";
         query += where + " "  + group_by +  " order by dimf.month";
     }else if(group == 'day'){
         group_by = "GROUP BY day_of_week_long_label_eng";
         query += queryOperation+ ", dimf.day_of_week_long_label_eng as label from formula_medica fm join dim_fecha dimf ";
         query +="on fm.key_date = dimf.key_date ";
         query += where + " "  + group_by +  " order by label";
     }
     console.log(query);
     const client = new pg.Client(config);
     client.connect();
 
     client.query(query)
         .then(function(result){
             client.end();
 
             return res.status(200).json(result.rows);
         })
         .catch(function(err){
             client.end();
 
             return res.status(500).json({message: err});
         })

}

exports.recipes = function(req, res){
    var month, year, limit;
    var queryOperation = "";
    var group_by = "";
    var where = "";

   

    if(req.query.month !=null){
        month = req.query.month
    }else{
        month = null
    }
    if(req.query.year !=null){
        year = req.query.year;
    }else{
        year = null;
    }

    if(req.query.limit !=null){
        limit = req.query.limit; 
    }else{
        limit = 10;
    }
    

    if(year!= null && month !=null){
        where+=" where year=" +year + " and month=" + month; 
      
    }else if(year !=null && month == null){
        where+= " where year=" +year;
       
    }else if(year == null && month !=null){
        where+= " where month=" + month; 
       
    }

    var query = 'select  med.nombre as label, count(med_rec.key_medicamento)as op  from';
	query += ' medicamentos_recetados med_rec join dim_medicamento med';
	query += ' on med.key_medicamento = med_rec.key_medicamento';
	query += ' join dim_fecha dimf on dimf.key_date = med_rec.key_date';
    query += where + ' group by(med.nombre) order by op desc limit ' + limit;
    const client = new pg.Client(config);
     client.connect();
 
     client.query(query)
         .then(function(result){
             client.end();
 
             return res.status(200).json(result.rows);
         })
         .catch(function(err){
             client.end();
 
             return res.status(500).json({message: err});
         })

}
exports.getIPS = function(req, res){
    var query  = "SELECT key_ips, id_ips, nombre FROM dim_ips where key_ips != 0";
    const client = new pg.Client(config);
    client.connect();

    client.query(query)
        .then(function(result){
            client.end();

            return res.status(200).json(result.rows);
        })
        .catch(function(err){
            client.end();

            return res.status(500).json({message: err});
        })

}

exports.getDocs = function(req, res){
    var query  = "SELECT key_medico, cedula, nombre FROM dim_medico where key_medico != 0";
    const client = new pg.Client(config);
    client.connect();

    client.query(query)
        .then(function(result){
            client.end();

            return res.status(200).json(result.rows);
        })
        .catch(function(err){
            client.end();

            return res.status(500).json({message: err});
        })
}
exports.getMedicamento = function(req, res){
    var query  = "SELECT key_medicamento,  nombre FROM dim_medicamento where key_medicamento != 0";
    const client = new pg.Client(config);
    client.connect();

    client.query(query)
        .then(function(result){
            client.end();

            return res.status(200).json(result.rows);
        })
        .catch(function(err){
            client.end();

            return res.status(500).json({message: err});
        })
}
exports.getYears = function(req, res){
    
    var query  = "SELECT distinct year FROM dim_fecha order by year";
    const client = new pg.Client(config);
    client.connect();

    client.query(query)
        .then(function(result){
            client.end();

            return res.status(200).json(result.rows);
        })
        .catch(function(err){
            client.end();

            return res.status(500).json({message: err});
        })
}
