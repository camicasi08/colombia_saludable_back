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

exports.getMedicamentosRecetados = function(req, res){
    
    if(req.query.limit !=null){
        limit = req.query.limit; 
    }else{
        limit = 500;
    }

    var query  = "select tf.codigo_formula, mr.key_medicamento, med.nombre  from medicamentos_recetados mr join trans_formula tf"
	query+=" on tf.key_formula = mr.key_formula";
	query+= " join dim_medicamento med on mr.key_medicamento = med.key_medicamento limit " + limit;
    const client = new pg.Client(config);
    client.connect();

    client.query(query)
        .then(function(result){
            client.end();
            var formulas={};
            result.rows.map(function(item){
                try {
                    if(formulas[item.codigo_formula+""]!=undefined){
                        formulas[item.codigo_formula+""].push(
                            {id:item.key_medicamento, nombre:item.nombre}
                        )
                    }else{
                        formulas[item.codigo_formula+""] = []
                        formulas[item.codigo_formula+""].push(
                            {id:item.key_medicamento, nombre:item.nombre}
                        )
                    }
                } catch (error) {
                    console.log(error);
                }
               
            })
            //console.log(formulas);
            //return res.status(200).json(result.rows);
           
            var relations ={};
            for(var key in formulas){
               var form = formulas[key]
                if(form.length>1){
                    //console.log(form);
                    var source = form[0]
                    //console.log(source);
                    if(relations[source.id+"-"+source.nombre] !=undefined){
                        
                       for (let index = 1; index < form.length; index++) {
                           const element = form[index];
                           const add = element.id+"-"+element.nombre;
                           if(!relations[source.id+"-"+source.nombre].includes(add)){
                                relations[source.id+"-"+source.nombre].push(add);
                           }
                           
                       }
                    }else{
                        relations[source.id+"-"+source.nombre] = [];
                        for (let index = 1; index < form.length; index++) {
                            const element = form[index];
                            const add = element.id+"-"+element.nombre;
                         
                            if(!relations[source.id+"-"+source.nombre].includes(add)){
                                relations[source.id+"-"+source.nombre].push(add);
                            }
                           
                          
                            
                        }
                   }
                    
                }
            }

            var nodes =[];
            var nodesTmp=[];
            var edges =[];
            for(var key in relations){
                var relation = relations[key];
                var idNode = key.split("-")[0];
                var name = key.split("-")[1];
                if(!nodesTmp.includes(key)){
                    nodesTmp.push(key);
                    nodes.push({
                        data:{
                            id:idNode,
                            name: name,
                            faveColor: "#3189ed",
                            faveShape: 'rectangle'
                        }
                    })
                }
                
                relation.map(function(edge){
                    var targetId = edge.split("-")[0];
                    var targetName = edge.split("-")[1];
                    if(!nodesTmp.includes(edge)){
                        nodesTmp.push(edge);
                        nodes.push({
                            data:{
                                id:targetId,
                                name: targetName,
                                faveColor: "#3189ed",
                                faveShape: 'rectangle'
                            }
                        })
                        
                    }
                   
                    edges.push({
                       data:{
                            source: idNode,
                            target: targetId,
                            faveColor: "#394b60"
                       } 
                    })
                })
            }
            var setNodes = Array.from(new Set(nodes));
           //console.log(setNodes);
           var graph ={
               nodes : setNodes,
               edges : edges
           }
            return res.status(200).json(graph);
        })
        .catch(function(err){
            console.log(err);
            client.end();

            return res.status(500).json({message: err});
        })
}
