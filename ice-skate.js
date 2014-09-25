// ice-skate.js
// Define reports as json objects and use skate() to generate the SQL query. 
// For the report format check example_report.js
// TODO: Implement Dimensions

// processSchema: 
// returns an object with relations beween attrs and relations between
// attrs and metrics
function processSchema(attrs,metrics,dimensions){	
	var a_rel = [];
	var m_rel = [];

	// loop attrs
	for(var i = 0; i < attrs.length; i++){
		var attrname =  attrs[i].name,		  // current attr name
			 attstables = attrs[i].table_list; // table list of current attr

		// relate current attr with every other one
		for(var j = 0; j < attrs.length && attrname != attrs[j].name; j++){
			var iname = attrname,
				jname = attrs[j].name,
				itbls = attstables, 
				jtbls = attrs[j].table_list

			for(var k = 0; k < itbls.length; k++){
				for(var p = 0; p < jtbls.length; p++){
					if(itbls[k].table_name == jtbls[p].table_name)
						a_rel.push({
							_id: Math.floor(Math.random()*100000000),
							attribute_a: attrs[i],
							attribute_a_column_name: itbls[k].id_column_name, 
							attribute_b: attrs[j],
							attribute_b_column_name: jtbls[p].id_column_name, 
							relation_table: itbls[k].table_name 
						});
				}
			}
		}

		// relate attrs with metrics
		for(var j = 0; j < metrics.length; j++){
			for(var k = 0; k < attstables.length; k++){
				if(metrics[j].base_table == attstables[k].table_name){

					var rel_table = {
						attribute: attrs[i],
						attribute_column_name: attstables[k].id_column_name,
						metric: metrics[j],
						relation_table_name: attstables[k].table_name
					}

					m_rel.push(rel_table);
				}
			}
		}
	} 

	return {
		attrs: attrs,
		metrics: metrics,
		attr_attr_relations:a_rel, 
		attr_metric_relations:m_rel
	};
}

// stringfyArray(['a','b','c']) == 'a,b,c'
// stringfyArray(['a','b','c'],['p','q','r']) == 'a p,b q,c r'
// stringfyArray(['a','b','c'],'AND') == 'a AND b AND c'
function stringfyArray(sa,sb){
	var s = '';
	if(sb === undefined){
		for(var i = 0; i < sa.length; i++)
			i == 0 ? s = sa[i] : s = s + ',' + sa[i];
	}
	else if(sb === 'AND' || sb === 'and'){
		for(var i = 0; i < sa.length; i++)
			i == 0 ? s = sa[i] : s = s + ' ' + sb + ' ' + sa[i];
	}
	else if (sa.length != sb.length){
		return s
	}
	else {
		for(var i = 0; i < sa.length; i++)
			i == 0 ? s = sa[i] + ' ' + sb[i] : s = s + ',' + sa[i] + ' ' + sb[i];		
	}
	return s;
}


// generateMetricsQueries: returns array with queries for metrics
// 	SELECT <attrs>, <aggregation function of metric 1>
//	FROM <base table 1>
//	GROUP BY <attrs>
function generateMetricsQueries(m_rel){
	var	t = [],		// list of tables to query
		a = [],		// list of attrs column names, in the same pos as the table
		an = [],	// list of attrs names (used as aliases)
		m = [],		// list of metrcis, in the same pos as the table
		mn = [],	// list of metrics names (used as aliases)
		q = [];		// list of queries generated

	// populate array t,a,m
	for(var i = 0; i < m_rel.length; i++){
		var rel_table = m_rel[i],
			pos = t.indexOf(rel_table.relation_table_name);

		if(pos == -1){
			t.push(rel_table.relation_table_name);
			
			a[t.length-1] = [];
			an[t.length-1] = [];
			m[t.length-1] = [];
			mn[t.length-1] = [];

			a[t.length-1].push(rel_table.attribute_column_name);
			an[t.length-1].push(rel_table.attribute.name);
			m[t.length-1].push(rel_table.metric.agg_function);
			mn[t.length-1].push(rel_table.metric.name);
		}
		else { // table already in t
			if(a[pos].indexOf(rel_table.attribute_column_name) == -1) 
				a[pos].push(rel_table.attribute_column_name);
			if(an[pos].indexOf(rel_table.attribute.name) == -1) 
				an[pos].push(rel_table.attribute.name);
			if(m[pos].indexOf(rel_table.metric.agg_function) == -1)
				m[pos].push(rel_table.metric.agg_function);
			if(mn[pos].indexOf(rel_table.metric.name) == -1)
				mn[pos].push(rel_table.metric.name);	
		}
	}

	// populate q (generate queries)
	for(var i = 0; i < t.length; i++){
		q[i] = 
			'SELECT ' + stringfyArray(a[i],an[i]) +',' + stringfyArray(m[i],mn[i]) +
			' FROM ' + t[i] +
			' GROUP BY ' + stringfyArray(a[i]);
	}

	return {queries:q,attrs:a};
}

// bridge: attribute shared by two attribute-relation tables
// Ai exists in Tp and Aj exists in Tq, then Ai = Aj (bridge)
function findBridges(a_rel){
	var b = []; // bridges list
	for(var i = 0; i < a_rel.length; i++){
		for(var j = 0; j < a_rel.length && a_rel[i]._id != a_rel[j]._id; j++){
			// if attribute shared for different tables		
			if(a_rel[i].attribute_a.name == a_rel[j].attribute_a.name
				|| a_rel[i].attribute_a.name == a_rel[j].attribute_b.name){
				//attribute a is bridge between tables
				b.push({
					_attribute_bridge_name: a_rel[i].attribute_a.name,
					attr_bridge_col: [a_rel[i].attribute_a_column_name, a_rel[j].attribute_a_column_name],
					rel_tables: [a_rel[i].relation_table,a_rel[j].relation_table]
				});
			} 
			if(a_rel[i].attribute_b.name == a_rel[j].attribute_a.name
				|| a_rel[i].attribute_b.name == a_rel[j].attribute_b.name){
				//attribute b is bridge between tables
				b.push({
					_attribute_bridge_name: a_rel[i].attribute_b.name,
					attr_bridge_col: [a_rel[i].attribute_b_column_name,a_rel[j].attribute_b_column_name],
					rel_tables: [a_rel[i].relation_table,a_rel[j].relation_table]
				});
			} 
		}
	}
	return b;
}

// generateAttributeQuery: returns a string query for attrs
function generateAttributeQuery(a_rel){
	
	var f = [],
		s = [],
		w = [],
		sa, sb, wa, wb;

	// TODO: avoid repetition
	for(var i = 0; i < a_rel.length; i++){
		var rel_table = a_rel[i],
			attrs_a_name = a_rel[i].attribute_a.name,
			attrs_b_name = a_rel[i].attribute_b.name,
			attr_a_lk = a_rel[i].attribute_a.lookup_table,
			attr_a_lk_col = a_rel[i].attribute_a.id_column_name,
			attr_b_lk = a_rel[i].attribute_b.lookup_table,
			attr_b_lk_col = a_rel[i].attribute_b.id_column_name;

		// generate attribute list for 'SELECT DISTINCT'
		sa = 
			attr_a_lk + '.' + // table alias
			a_rel[i].attribute_a.id_column_name + ' ' + // column
			attrs_a_name; // column alias
		if(s.indexOf(sa) == -1)
			s.push(sa);

		sb = 
			attr_b_lk + '.' + // table alias
			a_rel[i].attribute_b.id_column_name + ' ' + // column
			attrs_b_name; // column alias
		if(s.indexOf(sb) == -1)
			s.push(sb);		

		// generate table list for 'FROM '
		if(f.indexOf(attr_a_lk) == -1)
			f.push(attr_a_lk);

		if(f.indexOf(attr_b_lk) == -1)
			f.push(attr_b_lk);

		if(f.indexOf(rel_table.relation_table) == -1)
			f.push(rel_table.relation_table);

		// generate join conditions for 'WHERE '

		// join lookup w/ relations table
		wa = 
			attr_a_lk + '.' + // lookup table alias
			attr_a_lk_col + // lookup column id
			' = ' +
			rel_table.relation_table + '.' + // rel table alias
			rel_table.attribute_a_column_name; // rel column id
		if(w.indexOf(wa) == -1)
			w.push(wa);

		wb = 
			attr_b_lk + '.' + // lookup table alias
			attr_b_lk_col + // lookup column id
			' = ' +
			rel_table.relation_table + '.' + // rel table alias
			rel_table.attribute_b_column_name; // rel column id
		if(w.indexOf(wb) == -1)
			w.push(wb);
	}


	// join conditions between relational tables (using bridges)
	var wr = '',
		bridges = findBridges(a_rel);
	for(var i = 0; i < bridges.length; i++){
		for(var j = 0; j < bridges[i].rel_tables.length - 1; j++){
			wr = bridges[i].rel_tables[j] + '.' + bridges[i].attr_bridge_col[j]
				+ ' = ' + bridges[i].rel_tables[j+1] + '.' + bridges[i].attr_bridge_col[j+1] 
		}
		if(w.indexOf(wr) == -1)
			w.push(wr);
	}

	// generate query
	var q = 'SELECT DISTINCT ' + stringfyArray(s) +
			' FROM ' + stringfyArray(f) +
			' WHERE ' + stringfyArray(w,'AND');
	return q;
}

function getAttrNames(attrs){
	var s = []
	for(var i = 0; i < attrs.length; i++){
		s.push(attrs[i].name);
	}
	return s;
}

function generateLeftOuterJoinConditions(attrs,j){
	var c = '(',
		attrNames = attrs[j];
	for(var i = 0; i < attrNames.length; i++){
		if(i != 0) c = c + ' AND ';
		c = c + 'A.' + attrNames[i] + ' = ' + 'M' + j + '.' + attrNames[i]
	}
	c = c + ')';
	return c;
}

// generateQuery: generates query string to execute
function generateQuery(processed_schema){

	var a_rel = processed_schema.attr_attr_relations,
		m_rel = processed_schema.attr_metric_relations,
		attrs = processed_schema.attrs;

	var attrq = generateAttributeQuery(a_rel),
		metrq = generateMetricsQueries(m_rel);

	var q = 'SELECT * FROM ( ' + attrq + ' ) A ';
	for(var i = 0; i < metrq.queries.length ;i++){
		q = q + ' left outer join ( ' + metrq.queries[i] + ') M' + i +
			' on '+ generateLeftOuterJoinConditions(metrq.attrs,i)
	}
	
	return q;
}

function skate(attrs,metrics){
	return generateQuery(processSchema(attrs,metrics));
}