# DOCUMENT CREATED BY SIMONAS AUDICKAS
# DOCUMENT PURPOSE: PROJECT REPORTING DASHBOARD


# IMPORTING ALL NECESSARY LIBRARIES

import requests
import pandas as pd
import json
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
import os
import pymysql
from datetime import date
import glob

# Getting todays date
current = date.today().strftime("%Y_%m_%d")

# accessing restricted information: host, database, username, etc.
with open(r'C:\Users\Simonas\Documents\links.txt', 'r') as f:
    c = json.loads(f.read())

hostname = c['parameters'][1]['hostname']
username = c['parameters'][1]['username']
password = c['parameters'][1]['password']
database = c['parameters'][1]['database']
print(hostname, username, password, database)

# Connecting to the syno_manager database
conn = mysql.connector.connect(host=hostname, user=username, passwd=password, db=database)
cur = conn.cursor()

# GETTING THE PROJECT DATA
cur.execute("""
    select 
	p.id as project_id, 
	concat(ppg.project_id,'/',ppg.id) as project_wave_id,
	ps.name as project_status,
	c.name as client_company, 
	(select c2.name from company c2 where c.account_of = c2.id and c2.name!='Demo Company' ) as managing_office,
	(select c6.name from company c6 where c6.id = (select c5.invoiced_by from company c5 where c5.id = (select c4.id from company c4 where c.account_of = c4.id))) as managing_company,
	cou.english_name as client_country,
	case when concat(u.first_name ,' ', u.last_name ) is null then 'No Account Manager'
	else concat(u.first_name ,' ', u.last_name ) 
	end as account_manager,
	p.created_at ,
	p.closed_at ,
	p.archived_at
from project p
left join project_status ps on p.project_status_id =ps.id 
left join project_product_group ppg on p.id=ppg.project_id 
left join project_account_view pa on p.id=pa.project_id
left join company c on pa.company_id = c.id
left join project_sales_product sp on p.id=sp.project_id 
left join sales_product_translation spt on sp.sales_product_id =spt.id 
left join currency cur on sp.currency_id = cur.id 
left join country cc on sp.country_id = cc.id 
left join `user` u on c.responsible_account_manager_id = u.id
left join country cou on c.country_id = cou.id
where ps.name in ('New','On hold', 'In progress', 'Completed') and sp.sales_product_id is not null and c.deleted_at IS NULL and p.archived_at is null /*c.name not like '%Testing%' and c.name !='Demo Company'*/
group by 1,2,3,4,5,6,7,8,9;""")
project_data = cur.fetchall()

# PUSHING THE PROJECT DATA TO THE DATAFRAME
project_data_df = pd.DataFrame(project_data)
project_data_df.columns = ['project_id', 'project_wave_id', 'project_status', 'client_company', 'managing_company',
                           'managing_office', 'client_country', 'account_manager', 'created_at', 'closed_at',
                           'archived_at']
project_data_df

# CHECKING IF THE PROJECT NUMBER IS UNIQUE
project_data_df[project_data_df.duplicated(subset=['project_wave_id'], keep=False)]

# PUSHING THE PROJECT DATA TO LIBRARY DATABASE
msql_connector = c['logins'][1]['ds_projects']
sqlEngine = create_engine(msql_connector, pool_recycle=3600)  # creating a connection to the database
dbConnection = sqlEngine.connect()
try:
    frame = project_data_df.to_sql('syno_project_data', dbConnection, if_exists='replace', index=False);
except ValueError as vx:  # error handling
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Table created successfully.")
finally:
    dbConnection.close()

# GETTING THE PRODUCT DATA FROM MANAGER DATABASE
cur.execute("""select 
	p.id as project_id, 
    concat(ppg.project_id,'/',ppg.id) as project_wave_id,
	sp.sales_product_id ,  
	spt.name as product,
    substring_index(spt.name,'-',1) as product_category,
	substring_index(spt.name,' - ',-1) as product_sub_category,
	sp.unit_price/100 as unit_price,
	c.iso_code as currency,
	case when c.iso_code = 'TRY' then ((sp.quantity*sp.unit_price)/100)*0.12
		when c.iso_code = 'CLP' then ((sp.quantity*sp.unit_price)/100)*0.00023
		when c.iso_code = 'USD' then ((sp.quantity*sp.unit_price)/100)*0.85
		when c.iso_code = 'GBP' then ((sp.quantity*sp.unit_price)/100)*1.10
		when c.iso_code = 'PLN' then ((sp.quantity*sp.unit_price)/100)*0.23
		when c.iso_code = 'KRW' then ((sp.quantity*sp.unit_price)/100)*0.00071
		when c.iso_code= 'SEK' then ((sp.quantity*sp.unit_price)/100)* 0.097
		when c.iso_code= 'NZD' then ((sp.quantity*sp.unit_price)/100)* 0.57
		when c.iso_code= 'ARS' then ((sp.quantity*sp.unit_price)/100)* 0.012
		when c.iso_code= 'EUR' then (sp.quantity*sp.unit_price)/100
	else null
	end as product_amount_eur, 
	(sp.quantity*sp.unit_price)/100 as original_product_amount,
	cc.english_name as target_country,
	sp.created_at ,
	sp.updated_at
from project p
left join project_status ps on p.project_status_id =ps.id
left join project_product_group ppg on p.id=ppg.project_id 
left join project_sales_product sp on p.id=sp.project_id 
left join sales_product_translation spt on sp.sales_product_id =spt.translatable_id
left join currency c on sp.currency_id = c.id 
left join country cc on sp.country_id = cc.id 
left join company ccc on p.company_id =ccc.id
left join opportunity o on p.opportunity_id = o.id
left join `user` u on o.employee_id = u.id 
where ps.name in ('New','On hold', 'In progress', 'Completed') and ccc.name not like '%Testing%' and ccc.name !='Demo Company' and sp.sales_product_id is not null and ccc.name !='Demo Company'
;""")
product_data = cur.fetchall()

# PUSHING THE PRODUCT DATA TO THE DATAFRAME
product_data_df = pd.DataFrame(product_data)
product_data_df.columns = ['project_id', 'project_wave_id', 'sales_product_id', 'product', 'product_category',
                           'product_sub_category', 'unit_price', 'currency', 'product_amount_eur',
                           'original_product_amount', 'target_country', 'created_at', 'updated_at']

# CONVERTING THE COLUMNS TO NUMERIC VALUES
product_data_df['unit_price'] = pd.to_numeric(product_data_df['unit_price'])
product_data_df['product_amount_eur'] = pd.to_numeric(product_data_df['product_amount_eur'])
product_data_df['original_product_amount'] = pd.to_numeric(product_data_df['original_product_amount'])

# PUSHING PRODUCT DATA TO THE LIBRARY DATABASE
msql_connector = c['logins'][1]['ds_projects']
sqlEngine = create_engine(msql_connector, pool_recycle=3600)  # creating a connection to the database
dbConnection = sqlEngine.connect()
try:
    frame = product_data_df.to_sql('syno_product_data', dbConnection, if_exists='replace', index=False);
except ValueError as vx:  # error handling
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Table created successfully.")
finally:
    dbConnection.close()  # closing the connection to the database

# GETTING THE PROJECT COST DATA FROM MANAGER DATABASE
cur.execute("""select 
            pc.id, 
            pc.project_id, 
            pctt.name as cost_type, 
            c.iso_code,
            pc.amount/100 as original_amount,
            case when c.iso_code =  'ARS' then (pc.amount/100)*0.012
                when c.iso_code =  'DOP' then (pc.amount/100)*0.015
                when c.iso_code =  'TRY' then (pc.amount/100)*0.12
                when c.iso_code =  'USD' then (pc.amount/100)*0.85
                when c.iso_code = 'EUR' then (pc.amount/100)
                else null
                end as cost_amount_eur,
            concat(u.first_name ,' ', u.last_name ) as created_by_user, 
            pc.created_at,
            pc.cost_type_id
    from project_cost pc
    left join currency c on pc.currency_id = c.id
    left join cost_type_translation pctt on pc.cost_type_id = pctt.translatable_id
    left join `user` u on pc.created_by_user_id = u.id;""")
cost_data = cur.fetchall()

# PUSHING PROJECT COST DATA TO THE DATAFRAME
cost_data_df = pd.DataFrame(cost_data)
cost_data_df.columns = ['id', 'project_id', 'cost_type', 'iso_code', 'original_amount', 'cost_amount_eur',
                        'created_by_user', 'created_at', 'project_cost_type_id']

# CONVERTING THE COLUMNS TO NUMERIC VALUES
cost_data_df['original_amount'] = pd.to_numeric(cost_data_df['original_amount'])
cost_data_df['cost_amount_eur'] = pd.to_numeric(cost_data_df['cost_amount_eur'])

# PUSHING PROJECT COST DATA TO LIBRARY DATABASE
msql_connector = c['logins'][1]['ds_projects']
sqlEngine = create_engine(msql_connector, pool_recycle=3600)  # creating a connection to the database
dbConnection = sqlEngine.connect()
try:
    frame = cost_data_df.to_sql('syno_cost_data', dbConnection, if_exists='replace', index=False);
except ValueError as vx:  # error handling
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Table created successfully.")
finally:
    dbConnection.close()  # closing the connection to the database

# GETTING THE PROJECT TIME DATA FROM MANAGER DATABASE
cur.execute("""select ptl.id,project_id,
#ptl.user_id,
#ptl.created_by_user_id,
ptl.hours,
case when ptl.minutes = 15 then 0.25
			when ptl.minutes = 30 then 0.5
			when ptl.minutes = 45 then 0.75
			else ptl.minutes
		end as hour_part,
ptl.created_at,
ptl.deleted_at, 
concat(u.first_name ,' ', u.last_name ) as created_by_user
from project_time_log ptl 
left join `user` u on ptl.user_id = u.id;""")
time_data = cur.fetchall()

# CLEANING PROJECT TIME DATA
time_data_df = pd.DataFrame(time_data).fillna(0)
time_data_df.columns = ['id', 'project_id', 'hours', 'hour_part', 'created_at', 'deleted_at', 'created_by_user']
time_data_df['hours'] = pd.to_numeric(time_data_df['hours'])
time_data_df['hour_part'] = pd.to_numeric(time_data_df['hour_part'])
time_data_df['total_time'] = time_data_df['hours'] + time_data_df['hour_part']

# PUSHING PROJECT TIME DATA TO LIBRARY DATABASE
msql_connector = c['logins'][1]['ds_projects']
sqlEngine = create_engine(msql_connector, pool_recycle=3600)  # creating a connection to the database
dbConnection = sqlEngine.connect()
try:
    frame = time_data_df.to_sql('syno_time_data', dbConnection, if_exists='replace', index=False)
except ValueError as vx:  # error handling
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Table created successfully.")
finally:
    dbConnection.close()  # closing the connection to the database

# Getting INVOICE data from the DB
cur.execute("""select 
		p.id as project_id, 
		p.project_code, 
		i.id as invoice_id, 
		i.`number` as invoice_number , 
		ist.name as invoice_status, 
		ip.total/100 as invoice_amount, 
		ip.quantity, 
		ip.unit_price/100 as unit_price,
		spt.name as sales_product_name ,ipc.code, ipc.description
from project p
left join project_invoice pi on p.id = pi.project_id 
left join invoice i on pi.invoice_id = i.id
left join invoice_product ip on i.id=ip.invoice_id 
left join invoice_product_code ipc on ip.invoice_product_code_id = ipc.id 
left join invoice_status_translation ist on i.invoice_status_id = ist.id
left join sales_product_invoice_product_code spipc on ip.invoice_product_code_id = spipc.invoice_product_code_id 
left join sales_product_translation  spt on spipc.sales_product_id = spt.translatable_id 
where ip.total is not null
group by 1,2,3,4,5,6,7,8,9,10
order by 1 asc;
""")
invoice_data = cur.fetchall()

invoice_data_df = pd.DataFrame(invoice_data).fillna(0)
invoice_data_df.columns = ['project_id', 'project_code', 'invoice_id', 'invoice_number', 'invoice_status',
                           'invoice_amount', 'quantity', 'unit_price', 'sales_product_name', 'code', 'description']
invoice_data_df['invoice_amount'] = pd.to_numeric(invoice_data_df['invoice_amount'])
invoice_data_df['quantity'] = pd.to_numeric(invoice_data_df['quantity'])
invoice_data_df['unit_price'] = pd.to_numeric(invoice_data_df['unit_price'])

# PUSHING PROJECT INVOICE DATA TO LIBRARY DATABASE
msql_connector = c['logins'][1]['ds_projects']
sqlEngine = create_engine(msql_connector, pool_recycle=3600)  # creating a connection to the database
dbConnection = sqlEngine.connect()
try:
    frame = invoice_data_df.to_sql('syno_invoice_data', dbConnection, if_exists='replace', index=False);
except ValueError as vx:  # error handling
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Table created successfully.")
finally:
    dbConnection.close()  # closing the connection to the database

# ##Data export
cur.execute("""select p.id,
                concat(ppg.project_id,'/',ppg.id) as project_wave_id,
                p.name as project_name,
		c2.name as client_name,
		pi.invoice_id,
		i.`number` as invoice_number,
		ist.name as invoice_status,
		i.submit_date, 
		ip.project_code, 
		psp.sales_product_id, 
		spt.name as product,
		ip.invoice_currency ,
		ta.sales_amount, 
		ip.invoice_amount,
		ta.currency_id as sales_currency,
		case when ptl.hours is not null then concat(ptl.hours,':',ptl.minutes)
		else concat(0, ':',ptl.minutes)
		end as time_log
from project p
left join project_status ps on p.project_status_id = ps.id
left join project_sales_product psp on p.id=psp.project_id 
left join project_invoice pi on p.id=pi.project_id
left join invoice i on pi.invoice_id = i.id
left join (select 
				invoice_id, 
				project_code, 
				sales_product_id, 
				currency_id as invoice_currency,
				case when currency_id = 12 then round((sum(total)/100)*0.85,2)
				else sum(total)/100 
				end as invoice_amount,
				deleted_at 
			from invoice_product 
			group by 1,2,3) ip on pi.invoice_id = ip.invoice_id and psp.sales_product_id = ip.sales_product_id
left join (select 
				project_id, 
				sales_product_id, 
				case when psp.currency_id =12 then round((sum(quantity * unit_price)/100)*0.85,2) 
					when psp.currency_id =34 then round((sum(quantity * unit_price)/100)*0.00070,2)
					when psp.currency_id =21 then round((sum(quantity * unit_price)/100)*1.12,2)
					when psp.currency_id =20 then round((sum(quantity * unit_price)/100)*0.11,2)
					when psp.currency_id =8 then round((sum(quantity * unit_price)/100)*0.23,2)
					when psp.currency_id =4 then round((sum(quantity * unit_price)/100)*0.0079,2)
					when psp.currency_id =28 then round((sum(quantity* unit_price)/100)*0.56,2) /*new_zealand dollar*/
					when psp.currency_id = 24 then round((sum(quantity* unit_price)/100)*0.011,2) /*argentinas peso*/
					when psp.currency_id = 9 then round((sum(quantity* unit_price)/100)*0.095,2) /*SEK*/
					when psp.currency_id = 23 then round((sum(quantity* unit_price)/100)*0.0011,2) /*chiles peso*/
					when psp.currency_id =3 then sum(quantity * unit_price)/100 /*euro*/
				else null end as sales_amount, 
                psp.currency_id 
			from project_sales_product psp 
			group by 1,2) ta on ta.project_id=p.id and ta.sales_product_id=psp.sales_product_id
left join project_account pa on p.id=pa.project_id
left join company c2 on pa.company_id = c2.id
left join invoice_status_translation ist on i.invoice_status_id = ist.id
left join sales_product_translation spt on psp.sales_product_id = spt.translatable_id
left join project_product_group ppg on p.id = ppg.project_id 
left join project_time_log ptl on p.id=ptl.project_id and ptl.project_product_group_id = ppg.id
where ps.name in ('New','On hold', 'In progress', 'Completed') and c2.name not like '%Testing%' and c2.name !='Demo Company' and psp.sales_product_id is not null and ip.deleted_at is null
group by 1,2,3,4,5,6,7,8,9,10,11,12;
""")
data_export = cur.fetchall()
data_export_df = pd.DataFrame(data_export)
data_export_df.columns = (
'project_id', 'project_wave_id', 'project_name', 'client_name', 'invoice_id', 'invoice_number', 'invoice_status',
'submit_date', 'project_code', 'sales_product_id', 'product', 'invoice_currency', 'sales_amount', 'invoice_amount',
'sales_currency', 'time_log')
data_export_df['invoice_amount'] = pd.to_numeric(data_export_df['invoice_amount'])
data_export_df['sales_amount'] = pd.to_numeric(data_export_df['sales_amount'])
data_export_df['submit_date'] = data_export_df['submit_date'].dt.date

# PUSHING PROJECT DATA EXPORT TO DATABASE
msql_connector = c['logins'][1]['ds_projects']
sqlEngine = create_engine(msql_connector, pool_recycle=3600)  # creating a connection to the database
dbConnection = sqlEngine.connect()
try:
    frame = data_export_df.to_sql('syno_data_export', dbConnection, if_exists='replace', index=False);
except ValueError as vx:  # error handling
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Table created successfully.")
finally:
    dbConnection.close()  # closing the connection to the database

# Project summary reporting (one line per project wave)
cur.execute("""select p.id as project_id, 
        concat(ppg.project_id,'/',ppg.id) as project_wave_id,
        (select c2.name from company c2 where c.account_of = c2.id and c2.name!='Demo Company' ) as managing_company,
		(select c6.name from company c6 where c6.id = (select c5.invoiced_by from company c5 where c5.id = (select c4.id from company c4 where c.account_of = c4.id))) as managing_office,
		c1.name as managing_company,
		case when concat(u.first_name ,' ', u.last_name ) is null then 'No Account Manager'
	    else concat(u.first_name ,' ', u.last_name ) 
	    end as account_manager,
		p.name as project_name, 
		group_concat(distinct c.name separator '/') as client,
		sales.total_amount as revenue,
		sales.sales_amount_eur,
		sales.sales_currency,
		ad.invoice_amount,
		ad.invoice_amount_eur,
		ad.invoice_currency,
		pc.cost_amount,
		pc.cost_amount_eur,
		pc.cost_currency,
		ptl.time_log,
		ad.invoice_status,
		ad.invoice_number,
		ad.submit_date as invoice_date,
		p.closed_at as project_close_date,
		sales.last_sales_date,
		COALESCE (ad.submit_date, sales.last_sales_date,p.closed_at, p.created_at) as project_date,
        pss.name as project_status
from project p 
left join project_status pss on p.project_status_id=pss.id
left join company c1 on p.company_id = c1.id
left join project_account pa on p.id=pa.project_id
left join project_product_group ppg on p.id=ppg.project_id 
left join `user` u on c1.responsible_account_manager_id = u.id
left join company c on pa.company_id =c.id
left join (select 	project_id,
					concat(project_id,'/',project_product_group_id) as project_wave_id,
					sum(quantity * unit_price/100) as total_amount, 
					currency_id,
					case when psp.currency_id =12 then round((sum(quantity * unit_price)/100)*0.85,2) 
					when psp.currency_id =34 then round((sum(quantity * unit_price)/100)*0.00070,2)
					when psp.currency_id =21 then round((sum(quantity * unit_price)/100)*1.12,2)
					when psp.currency_id =20 then round((sum(quantity * unit_price)/100)*0.11,2)
					when psp.currency_id =8 then round((sum(quantity * unit_price)/100)*0.23,2)
					when psp.currency_id =4 then round((sum(quantity * unit_price)/100)*0.0079,2)
					when psp.currency_id =28 then round((sum(quantity* unit_price)/100)*0.56,2) /*new_zealand dollar*/
					when psp.currency_id = 24 then round((sum(quantity* unit_price)/100)*0.011,2) /*argentinas peso*/
					when psp.currency_id = 9 then round((sum(quantity* unit_price)/100)*0.095,2) /*SEK*/
					when psp.currency_id = 23 then round((sum(quantity* unit_price)/100)*0.0011,2) /*chiles peso*/
					when psp.currency_id =3 then sum(quantity * unit_price)/100 /*euro*/
				else null end as sales_amount_eur, 
					c3.name as sales_currency,
				max(psp.created_at) as last_sales_date
			from project_sales_product psp 
			left join currency c3 on psp.currency_id = c3.id
			group by 1,2,4,6) as sales on sales.project_wave_id=concat(ppg.project_id,'/',ppg.id)
left join (select i.`number` as invoice_number, 
					i.submit_date, i.buyer_company_id,
					i.total/100 as invoice_amount, 
					ip.project_code, 
					psp.project_product_group_id, 
					psp.project_id, 
					concat(psp.project_id,'/', psp.project_product_group_id) as project_cost_id, 
					ist.name as invoice_status,
					c4.name as invoice_currency,
					case when i.currency_id =12 then round(i.total/100*0.85,2) 
					when i.currency_id =34 then round(i.total/100*0.00070,2)
					when i.currency_id =21 then round(i.total/100*1.12,2)
					when i.currency_id =20 then round(i.total/100*0.11,2)
					when i.currency_id =8 then round(i.total/100*0.23,2)
					when i.currency_id =4 then round(i.total/100*0.0079,2)
					when i.currency_id =28 then round(i.total/100*0.56,2) /*new_zealand dollar*/
					when i.currency_id = 24 then round(i.total/100*0.011,2) /*argentinas peso*/
					when i.currency_id = 9 then round(i.total/100*0.095,2) /*SEK*/
					when i.currency_id = 23 then round(i.total/100*0.0011,2) /*chiles peso*/
					when i.currency_id =3 then i.total/100 /*euro*/
				else null end as invoice_amount_eur
			from invoice i
			left join invoice_product ip on i.id=ip.invoice_id 
			left join project_sales_product psp on ip.project_sales_product_id = psp.id
			left join invoice_status_translation ist on i.invoice_status_id=ist.translatable_id
			left join currency c4 on i.currency_id = c4.id
			group by 1,2,3,4,5,6,7,8,9,10,11) ad on p.id= ad.project_id and ad.project_cost_id=concat(ppg.project_id,'/',ppg.id)
left join (select	concat(project_id,'/',project_product_group_id) as project_wave_id, 
		sum(amount/100) as cost_amount,
		sum(case when pc.currency_id =12 then round(((pc.amount)/100)*0.85,2) 
		when pc.currency_id =34 then round(((pc.amount)/100)*0.00070,2)
		when pc.currency_id =21 then round(((pc.amount)/100)*1.12,2)
		when pc.currency_id =20 then round(((pc.amount)/100)*0.11,2)
		when pc.currency_id =8 then round(((pc.amount)/100)*0.23,2)
		when pc.currency_id =4 then round(((pc.amount)/100)*0.0079,2)
		when pc.currency_id =28 then round(((pc.amount)/100)*0.56,2) /*new_zealand dollar*/
		when pc.currency_id = 24 then round(((pc.amount)/100)*0.011,2) /*argentinas peso*/
		when pc.currency_id = 9 then round(((pc.amount)/100)*0.095,2) /*SEK*/
		when pc.currency_id = 23 then round(((pc.amount)/100)*0.0011,2) /*chiles peso*/
		when pc.currency_id = 26 then round(((pc.amount)/100)*0.015,2) /*Dominican peso*/
		when pc.currency_id =3 then (pc.amount)/100 /*euro*/
	else null end) as cost_amount_eur,
		c2.name as cost_currency
from project_cost pc
left join currency c2 on pc.currency_id = c2.id
group by 1) as pc on pc.project_wave_id = concat(ppg.project_id,'/',ppg.id)
left join (select concat(project_id,'/',project_product_group_id) as project_wave_id,
					case when sum(hours) is null then sum(minutes)/60
					else (sum(hours)*60+sum(minutes))/60 
					end as time_log
			from project_time_log
			group by 1) as ptl on ptl.project_wave_id=concat(ppg.project_id,'/',ppg.id)
where p.archived_at is null and p.deleted_at is null /*and ad.invoice_number is null or ad.invoice_number not like '%TEMP%'*/
group by 1,2,3,4,5,6,7,9,10,11;
""")
project_summary = cur.fetchall()

project_summary_df = pd.DataFrame(project_summary)
project_summary_df.columns = (
'project_id', 'project_wave_id', 'managing_office', 'managing_company', 'account_manager', 'managing_officer','project_name', 'client',
'revenue', 'sales_amount_eur', 'sales_currency', 'invoice_amount', 'invoice_amount_eur', 'invoice_currency',
'cost_amount', 'cost_amount_eur', 'cost_currency', 'time_log', 'invoice_status', 'invoice_number', 'submit_date',
'project_close_date', 'last_sales_date', 'project_date', 'project_status')
project_summary_df['revenue'] = pd.to_numeric(project_summary_df['revenue'])
project_summary_df['sales_amount_eur'] = pd.to_numeric(project_summary_df['sales_amount_eur'])
project_summary_df['invoice_amount'] = pd.to_numeric(project_summary_df['invoice_amount'])
project_summary_df['invoice_amount_eur'] = pd.to_numeric(project_summary_df['invoice_amount_eur'])
project_summary_df['cost_amount'] = pd.to_numeric(project_summary_df['cost_amount'])
project_summary_df['cost_amount_eur'] = pd.to_numeric(project_summary_df['cost_amount_eur'])
project_summary_df['time_log'] = pd.to_numeric(project_summary_df['time_log'])
project_summary_df['submit_date'] = project_summary_df['submit_date'].dt.date
project_summary_df['project_date'] = project_summary_df['project_date'].dt.date
project_summary_df['last_sales_date'] = project_summary_df['last_sales_date'].dt.date
project_summary_df['project_close_date'] = project_summary_df['project_close_date'].dt.date

# PUSHING PROJECT SUMMARY DATA TO REPORTING DATABASE
msql_connector = c['logins'][1]['ds_projects']
sqlEngine = create_engine(msql_connector, pool_recycle=3600)  # creating a connection to the database
dbConnection = sqlEngine.connect()
try:
    frame = project_summary_df.to_sql('syno_project_summary_data_export', dbConnection, if_exists='replace',
                                      index=False);
except ValueError as vx:  # error handling
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Summary Table created successfully.")
finally:
    dbConnection.close()  # closing the connection to the database

# ## Cost Dump
cur.execute("""select 
		p.id as project_id, 
        concat(ppg.project_id,'/',ppg.id) as project_cost_id,
        case when p.id is not null then 'Project'
        else null
        end as cost_center,
		c1.name as managing_company,
		p.name as project_name, 
        p.project_code,
		group_concat(distinct c.name separator '/') as client,
		c2.name as invoiced_company,
		ad.submit_date,
        ad.invoice_number,
        ad.invoice_status,
		ad.invoice_amount,
		pc.amount/100 as cost_amount, 
		c3.name as currency,
		case when pc.currency_id =12 then round((sum(pc.amount)/100)*0.85,2) 
					when pc.currency_id =34 then round((sum(pc.amount)/100)*0.00070,2)
					when pc.currency_id =21 then round((sum(pc.amount)/100)*1.12,2)
					when pc.currency_id =20 then round((sum(pc.amount)/100)*0.11,2)
					when pc.currency_id =8 then round((sum(pc.amount)/100)*0.23,2)
					when pc.currency_id =4 then round((sum(pc.amount)/100)*0.0079,2)
					when pc.currency_id =28 then round((sum(pc.amount)/100)*0.56,2) /*new_zealand dollar*/
					when pc.currency_id = 24 then round((sum(pc.amount)/100)*0.011,2) /*argentinas peso*/
					when pc.currency_id = 9 then round((sum(pc.amount)/100)*0.095,2) /*SEK*/
					when pc.currency_id = 23 then round((sum(pc.amount)/100)*0.0011,2) /*chiles peso*/
					when pc.currency_id = 26 then round((sum(pc.amount)/100)*0.015,2) /*Dominican peso*/
					when pc.currency_id =3 then sum(pc.amount)/100 /*euro*/
				else null end as cost_amount_eur,
		pc.comment as cost_comment,
		pctt.name as cost_type,
		ptl.time_log
from project p
left join company c1 on p.company_id = c1.id
left join project_account pa on p.id=pa.project_id
left join project_product_group ppg on p.id=ppg.project_id 
left join company c on pa.company_id =c.id
left join project_cost pc on p.id = pc.project_id and pc.project_product_group_id =ppg.id
left join currency c3 on pc.currency_id = c3.id 
left join cost_type_translation pctt on pc.cost_type_id = pctt.translatable_id
left join (select i.`number` as invoice_number, 
                  i.submit_date, i.buyer_company_id,
                i.total/100 as invoice_amount, ip.project_code, psp.project_product_group_id, psp.project_id, 
                concat(psp.project_id,'/', psp.project_product_group_id) as project_cost_id, ist.name as invoice_status
            from invoice i
            left join invoice_product ip on i.id=ip.invoice_id 
            left join project_sales_product psp on ip.project_sales_product_id = psp.id
            left join invoice_status_translation ist on i.invoice_status_id=ist.translatable_id
            group by 1,2,3,4,5,6,7) ad on p.id= ad.project_id and ad.project_cost_id=concat(ppg.project_id,'/',ppg.id)
left join (select concat(project_id,'/',project_product_group_id) as project_wave_id,
					case when sum(hours) is null then sum(minutes)/60
					else (sum(hours)*60+sum(minutes))/60 
					end as time_log
			from project_time_log
			group by 1) as ptl on ptl.project_wave_id=concat(ppg.project_id,'/',ppg.id)
left join company c2 on ad.buyer_company_id=c2.id
group by 1,2,3,4,5,6,8,9,10,12,13,14
order by 1 asc;
""")
cost_dump = cur.fetchall()
cost_dump_df = pd.DataFrame(cost_dump)

cost_dump_df.columns = (
'project_id', 'project_cost_id', 'cost_center', 'managing_company', 'project_name', 'project_code', 'client',
'invoiced_company', 'invoice_date', 'invoice_number', 'invoice_status', 'invoice_amount', 'cost_amount',
'cost_currency', 'cost_amount_eur', 'cost_comment', 'cost_type', 'time_log')
cost_dump_df['invoice_amount'] = pd.to_numeric(cost_dump_df['invoice_amount'])
cost_dump_df['cost_amount'] = pd.to_numeric(cost_dump_df['cost_amount'])
cost_dump_df['cost_amount_eur'] = pd.to_numeric(cost_dump_df['cost_amount_eur'])
cost_dump_df['time_log'] = pd.to_numeric(cost_dump_df['time_log'])
cost_dump_df['invoice_date'] = cost_dump_df['invoice_date'].dt.date

# omnibus
cur.execute("""select 	oo.id,
		case when oo.id then null
		end as project_cost_id,
		case when oo.id is not null then 'Answers'
		else null
		end as cost_center,
		c.name as managing_company,
		oo.name as project_name,
		concat('A',oo.id) as project_code,
		cc.name as client,
		c4.name as invoiced_company,
		i.submit_date as invoice_date,
		i.`number` as invoice_number,
		ist.name as invoice_status,
		i.grand_total/100 as invoice_amount,
		ooc.amount/100 as cost_amount,
        c3.name as currency,
        case when ooc.currency_id =12 then round((sum(ooc.amount)/100)*0.85,2) 
					when ooc.currency_id =34 then round((sum(ooc.amount)/100)*0.00070,2)
					when ooc.currency_id =21 then round((sum(ooc.amount)/100)*1.12,2)
					when ooc.currency_id =20 then round((sum(ooc.amount)/100)*0.11,2)
					when ooc.currency_id =8 then round((sum(ooc.amount)/100)*0.23,2)
					when ooc.currency_id =4 then round((sum(ooc.amount)/100)*0.0079,2)
					when ooc.currency_id =28 then round((sum(ooc.amount)/100)*0.56,2) /*new_zealand dollar*/
					when ooc.currency_id = 24 then round((sum(ooc.amount)/100)*0.011,2) /*argentinas peso*/
					when ooc.currency_id = 9 then round((sum(ooc.amount)/100)*0.095,2) /*SEK*/
					when ooc.currency_id = 23 then round((sum(ooc.amount)/100)*0.0011,2) /*chiles peso*/
					when ooc.currency_id = 26 then round((sum(ooc.amount)/100)*0.015,2) /*Dominican peso*/
			else ooc.amount/100 /*euro*/
			end as cost_amount_eur,
		ooc.comment,
		ctt.name as cost_type,
		(sum(ootl.hours )*60+sum(ootl.minutes))/60 as time_log
		/*case when ooc.currency_id =12 then round((sum(ooc.amount)/100)*0.85,2) 
					when ooc.currency_id =34 then round((sum(ooc.amount)/100)*0.00070,2)
					when ooc.currency_id =21 then round((sum(ooc.amount)/100)*1.12,2)
					when ooc.currency_id =20 then round((sum(ooc.amount)/100)*0.11,2)
					when ooc.currency_id =8 then round((sum(ooc.amount)/100)*0.23,2)
					when ooc.currency_id =4 then round((sum(ooc.amount)/100)*0.0079,2)
					when ooc.currency_id =28 then round((sum(ooc.amount)/100)*0.56,2) new_zealand dollar
					when ooc.currency_id = 24 then round((sum(ooc.amount)/100)*0.011,2) argentinas peso
					when ooc.currency_id = 9 then round((sum(ooc.amount)/100)*0.095,2) SEK
					when ooc.currency_id = 23 then round((sum(ooc.amount)/100)*0.0011,2) chiles peso
					when ooc.currency_id = 26 then round((sum(ooc.amount)/100)*0.015,2) Dominican peso/
			else ooc.amount/100 /*euro
			end as cost_amount,
		,
		oot.credit/100,
		oot.debit/100,
		*/		
from syno_manager.omnibus_order oo
left join syno_manager.omnibus_order_status_translation oost on oo.status_id = oost.id 
left join syno_manager.company c on c.id= oo.managing_company_id
left join syno_manager.company c2 on c.invoiced_by = c2.id
left join syno_manager.company cc on oo.company_id = cc.id
left join syno_manager.omnibus_order_cost ooc on oo.id=ooc.omnibus_order_id 
left join syno_manager.currency c3 on ooc.currency_id = c3.id
left join syno_manager.omnibus_order_transaction oot on oo.id= oot.id
left join (SELECT oo.id as order_id, bi.* 
            FROM omnibus_order oo, omnibus_order_item ooi, billing_item bi, billing_item_attribute bia, billing_item_attribute_value biav 
            WHERE oo.id = ooi.omnibus_order_id
            AND ooi.id = biav.value
            AND biav.billing_item_attribute_id = bia.id
            AND bia.name = 'omnibus_order_item_id'
            AND biav.billing_item_id = bi.id) a on oo.id=a.order_id
left join syno_manager.invoice i on a.invoice_id = i.id 
left join syno_manager.invoice_status_translation ist on i.invoice_status_id = ist.translatable_id 
left join syno_manager.company c4 on i.buyer_company_id = c4.id
/*left join syno_manager.sales_product_translation spt on oo.sales_product_id = spt.id*/
left join syno_manager.cost_type_translation ctt on ooc.cost_type_id = ctt.translatable_id 
left join syno_manager.omnibus_order_time_log ootl on oo.id = ootl.omnibus_order_id 
where oo.archived_at is null
group by 1,2,3,4,5,6,7,8,9,10, 11, 12, 13, 16,17;
""")
omnibus_dump = cur.fetchall()
omnibus_dump = pd.DataFrame(omnibus_dump)
omnibus_dump.columns = (
'project_id', 'project_cost_id', 'cost_center', 'managing_company', 'project_name', 'project_code', 'client',
'invoiced_company', 'invoice_date', 'invoice_number', 'invoice_status', 'invoice_amount', 'cost_amount',
'cost_currency', 'cost_amount_eur', 'cost_comment', 'cost_type', 'time_log')
omnibus_dump['invoice_amount'] = pd.to_numeric(omnibus_dump['invoice_amount'])
omnibus_dump['cost_amount'] = pd.to_numeric(omnibus_dump['cost_amount'])
omnibus_dump['cost_amount_eur'] = pd.to_numeric(omnibus_dump['cost_amount_eur'])
omnibus_dump['time_log'] = pd.to_numeric(omnibus_dump['time_log'])
omnibus_dump['invoice_date'] = omnibus_dump['invoice_date'].dt.date

# merge project and omnibus data
projects_omnibuses = [cost_dump_df, omnibus_dump]
projects_omnibuses = pd.concat(projects_omnibuses)

# PUSHING PROJECT TIME DATA TO LIBRARY DATABASE
msql_connector = c['logins'][1]['ds_projects']
sqlEngine = create_engine(msql_connector, pool_recycle=3600)  # creating a connection to the database

dbConnection = sqlEngine.connect()
try:
    frame = projects_omnibuses.to_sql('syno_cost_data_export', dbConnection, if_exists='replace', index=False);
except ValueError as vx:  # error handling
    print(vx)

except Exception as ex:
    print(ex)
else:
    print("Cost Dump Table created successfully.")
finally:
    dbConnection.close()  # closing the connection to the database

# FILES FOR EASY ACCESS
# removes the old files first
cost_files = glob.glob(r"G:\My Drive\Internal_project_reporting\cost_dump\*")
for cost_f in cost_files:
    os.remove(cost_f)
    print(cost_f + "Removed!")

# writes the new file into two instances
projects_omnibuses.to_excel(
    r"C:\Users\Simonas\Documents\ds_project_files\result_files\syno_project_reporting\cost_dump\cost_dump_" + current + ".xlsx",
    index=False)
projects_omnibuses.to_excel(r"G:\My Drive\Internal_project_reporting\cost_dump\cost_dump_" + current + ".xlsx",
                            index=False)
print("cost_dump_" + current + ".xlsx" + " " + "written to disk")

# Buhalterija cost dump
cur.execute("""select 
		p.id as project_id, 
        concat(ppg.project_id,'/',ppg.id) as project_cost_id,
		c1.name as managing_company,
		p.name as project_name, 
		group_concat(distinct c.name separator '/') as client,
		c2.name as invoiced_company,
		psp2.quantity, 
		psp2.unit_price,
		psp2.total_sales,
		psp2.product_name,
		ad.submit_date,
        ad.invoice_number,
        ad.invoice_status,
		ad.invoice_amount,
		pc.amount/100 as cost_amount, 
		pc.comment as cost_comment,
		pctt.name as cost_type,
		ptl.time_log
from project p
left join company c1 on p.company_id = c1.id
left join project_account pa on p.id=pa.project_id
left join project_product_group ppg on p.id=ppg.project_id 
left join company c on pa.company_id =c.id
left join project_cost pc on p.id = pc.project_id and pc.project_product_group_id =ppg.id
left join (select 
				project_id, 
				sum(quantity) as quantity, 
				unit_price/100 as unit_price, 
				sum(quantity * unit_price/100) as total_sales, 
				project_product_group_id, 
				concat(project_id,'/',project_product_group_id) as project_cost_id,
				spt.name as product_name
			from project_sales_product psp2
			left join sales_product_translation spt on psp2.sales_product_id = spt.translatable_id 
			group by 1,5,7) as psp2 on psp2.project_cost_id=concat(ppg.project_id,'/',ppg.id)
left join cost_type_translation pctt on pc.cost_type_id = pctt.translatable_id
left join (select i.`number` as invoice_number, i.submit_date, i.buyer_company_id,
				i.total/100 as invoice_amount, ip.project_code, psp.project_product_group_id, psp.project_id, 
				concat(psp.project_id,'/', psp.project_product_group_id) as project_cost_id, ist.name as invoice_status
			from invoice i
			left join invoice_product ip on i.id=ip.invoice_id 
			left join project_sales_product psp on ip.project_sales_product_id = psp.id
			left join invoice_status_translation ist on i.invoice_status_id=ist.translatable_id
			group by 1,2,3,4,5,6,7) ad on p.id= ad.project_id and ad.project_cost_id=concat(ppg.project_id,'/',ppg.id)
left join (select concat(project_id,'/',project_product_group_id) as project_wave_id,
					case when sum(hours) is null then sum(minutes)/60
					else (sum(hours)*60+sum(minutes))/60 
					end as time_log
			from project_time_log
			group by 1) as ptl on ptl.project_wave_id=concat(ppg.project_id,'/',ppg.id)
left join company c2 on ad.buyer_company_id=c2.id
group by 1,2,3,4,6,10,11,12,13,15,16,17
order by 1 asc;""")
buh = cur.fetchall()
df_buh = pd.DataFrame(buh)
df_buh.columns = (
'project_id', 'project_cost_id', 'managing_company', 'project_name', 'client', 'invoiced_company', 'quantity',
'unit_price', 'total_sales', 'product_name', 'submit_date', 'invoice_number', 'invoice_status', 'invoice_amount',
'cost_amount', 'cost_comment', 'cost_type', 'time_log')
df_buh['quantity'] = pd.to_numeric(df_buh['quantity'])
df_buh['unit_price'] = pd.to_numeric(df_buh['unit_price'])
df_buh['total_sales'] = pd.to_numeric(df_buh['total_sales'])
df_buh['submit_date'] = df_buh['submit_date'].dt.date
df_buh['invoice_amount'] = pd.to_numeric(df_buh['invoice_amount'])
df_buh['cost_amount'] = pd.to_numeric(df_buh['cost_amount'])
df_buh['time_log'] = pd.to_numeric(df_buh['time_log'])

# removes the old files from the disk
buh_files = glob.glob(r"G:\My Drive\Internal_project_reporting\buh_dump\*")
for buh_f in buh_files:
    os.remove(buh_f)
    print(buh_f + " " + "Removed!")

# writes new file into two instance
df_buh.to_excel(
    r"C:\Users\Simonas\Documents\ds_project_files\result_files\syno_project_reporting\buh_dump\project_cost_dump_buh_" + current + ".xlsx",
    index=False)
df_buh.to_excel(r"G:\My Drive\Internal_project_reporting\buh_dump\project_cost_dump_buh_" + current + ".xlsx",
                index=False)

# PUSHING Buh cost dump To Reporting DB
msql_connector = c['logins'][1]['ds_projects']
sqlEngine = create_engine(msql_connector, pool_recycle=3600)  # creating a connection to the database
dbConnection = sqlEngine.connect()
try:
    frame = df_buh.to_sql('syno_buh_data_export', dbConnection, if_exists='replace', index=False);
except ValueError as vx:  # error handling
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Table created successfully.")
finally:
    dbConnection.close()  # closing the connection to the database

# data_Export_Extended with dynamic currency

with open(r'C:\Users\Simonas\Documents\links.txt', 'r') as f:
    c = json.loads(f.read())

hostname = c['parameters'][3]['hostname']
username = c['parameters'][3]['username']
password = c['parameters'][3]['password']
database = 'currencies'
print(hostname, username, password, database)
conn1 = mysql.connector.connect(host=hostname, user=username, passwd=password, db=database)
# Simple routine to run a query on a database and print the results:
cur1 = conn1.cursor()

cur1.execute("""select * from currencies.weekly_avg""")
currencies_data = cur1.fetchall()
df_cur = pd.DataFrame(currencies_data)
df_cur[0] = df_cur[0].dt.date

df_cur[0] = df_cur[0].apply(pd.to_datetime)
df_cur1 = df_cur.loc[df_cur[0] > '2018-01-01']
df_cur1 = df_cur1[[1, 3, 4, 6]]
df_cur1 = df_cur1.groupby(by=[1, 3]).mean().round(2)
df_cur1 = df_cur1.reset_index()
df_cur1.columns = ['currency_code', 'week_nr', 'rate_avg']

df_cur2 = df_cur1.copy()
df_cur2.columns = ['currency_code', 'week_nr', 'rate_avg_inv']


cur.execute("""select p.id as project_id, 
        concat(ppg.project_id,'/',ppg.id) as project_wave_id,
        (select c2.name from company c2 where c.account_of = c2.id and c2.name!='Demo Company' ) as client_managing_company,
		(select c6.name from company c6 where c6.id = (select c5.invoiced_by from company c5 where c5.id = (select c4.id from company c4 where c.account_of = c4.id))) as managing_office,
		c1.name as managing_company,
		p.name as project_name, 
		group_concat(distinct c.name separator '/') as client,
		sales.product,
		sales.total_amount as revenue,
		sales.sales_amount_eur,
		sales.sales_currency,
        sales.currency_code,
		ad.invoice_amount,
		ad.invoice_amount_eur,
		ad.invoice_currency,
        ad.inv_currency_code,
		pc.cost_amount,
		pc.cost_amount_eur,
		pc.cost_currency,
		ptl.time_log,
		ad.invoice_status,
		ad.invoice_number,
		ad.submit_date as invoice_date,
		p.closed_at as project_close_date,
		sales.last_sales_dt,
		COALESCE (ad.submit_date, sales.last_sales_dt,p.closed_at, p.created_at) as project_date
from project p 
left join company c1 on p.company_id = c1.id
left join project_account pa on p.id=pa.project_id
left join project_product_group ppg on p.id=ppg.project_id 
left join company c on pa.company_id =c.id
left join (select 	project_id,
					concat(project_id,'/',project_product_group_id) as project_wave_id,
					spt.name as product,
					sum(quantity * unit_price/100) as total_amount, 
					currency_id,
                    c3.iso_code as currency_code,
					case when psp.currency_id =12 then round((sum(quantity * unit_price)/100)*0.85,2) 
					when psp.currency_id =34 then round((sum(quantity * unit_price)/100)*0.00070,2)
					when psp.currency_id =21 then round((sum(quantity * unit_price)/100)*1.12,2)
					when psp.currency_id =20 then round((sum(quantity * unit_price)/100)*0.11,2)
					when psp.currency_id =8 then round((sum(quantity * unit_price)/100)*0.23,2)
					when psp.currency_id =4 then round((sum(quantity * unit_price)/100)*0.0079,2)
					when psp.currency_id =28 then round((sum(quantity* unit_price)/100)*0.56,2) /*new_zealand dollar*/
					when psp.currency_id = 24 then round((sum(quantity* unit_price)/100)*0.011,2) /*argentinas peso*/
					when psp.currency_id = 9 then round((sum(quantity* unit_price)/100)*0.095,2) /*SEK*/
					when psp.currency_id = 23 then round((sum(quantity* unit_price)/100)*0.0011,2) /*chiles peso*/
					when psp.currency_id =3 then sum(quantity * unit_price)/100 /*euro*/
				else null end as sales_amount_eur, 
					c3.name as sales_currency,
				max(psp.created_at) as last_sales_dt
			from project_sales_product psp 
			left join currency c3 on psp.currency_id = c3.id
			left join sales_product_translation spt on psp.sales_product_id = spt.translatable_id 
			group by 1,2,3,5) as sales on sales.project_wave_id=concat(ppg.project_id,'/',ppg.id)
left join (select i.`number` as invoice_number, 
					i.submit_date, i.buyer_company_id,
					sum(ip.total/100) as invoice_amount, 
					ip.project_code, 
					psp.project_product_group_id, 
					psp.project_id, 
					concat(psp.project_id,'/', psp.project_product_group_id) as project_cost_id,
					spt2.name as product,
					ist.name as invoice_status,
					c4.name as invoice_currency,
                    c4.iso_code as inv_currency_code,
					case when i.currency_id =12 then round(sum(ip.total/100)*0.85,2) 
					when i.currency_id =34 then round(sum(ip.total/100)*0.00070,2)
					when i.currency_id =21 then round(sum(ip.total/100)*1.12,2)
					when i.currency_id =20 then round(sum(ip.total/100)*0.11,2)
					when i.currency_id =8 then round(sum(ip.total/100)*0.23,2)
					when i.currency_id =4 then round(sum(ip.total/100)*0.0079,2)
					when i.currency_id =28 then round(sum(ip.total/100)*0.56,2) /*new_zealand dollar*/
					when i.currency_id = 24 then round(sum(ip.total/100)*0.011,2) /*argentinas peso*/
					when i.currency_id = 9 then round(sum(ip.total/100)*0.095,2) /*SEK*/
					when i.currency_id = 23 then round(sum(ip.total/100)*0.0011,2) /*chiles peso*/
					when i.currency_id =3 then sum(ip.total/100) /*euro*/
				else null end as invoice_amount_eur,
				max(psp.created_at) as last_sales_date
			from invoice i
			left join invoice_product ip on i.id=ip.invoice_id 
			left join project_sales_product psp on ip.project_sales_product_id = psp.id
			left join sales_product_translation spt2 on psp.sales_product_id = spt2.translatable_id 
			left join invoice_status_translation ist on i.invoice_status_id=ist.translatable_id
			left join currency c4 on i.currency_id = c4.id
			group by 1,2,3,5,6,7,8,9,10,11) ad on p.id= ad.project_id and ad.project_cost_id=concat(ppg.project_id,'/',ppg.id) and sales.product=ad.product
left join (select	concat(project_id,'/',project_product_group_id) as project_wave_id, 
		sum(amount/100) as cost_amount,
		sum(case when pc.currency_id =12 then round(((pc.amount)/100)*0.85,2) 
		when pc.currency_id =34 then round(((pc.amount)/100)*0.00070,2)
		when pc.currency_id =21 then round(((pc.amount)/100)*1.12,2)
		when pc.currency_id =20 then round(((pc.amount)/100)*0.11,2)
		when pc.currency_id =8 then round(((pc.amount)/100)*0.23,2)
		when pc.currency_id =4 then round(((pc.amount)/100)*0.0079,2)
		when pc.currency_id =28 then round(((pc.amount)/100)*0.56,2) /*new_zealand dollar*/
		when pc.currency_id = 24 then round(((pc.amount)/100)*0.011,2) /*argentinas peso*/
		when pc.currency_id = 9 then round(((pc.amount)/100)*0.095,2) /*SEK*/
		when pc.currency_id = 23 then round(((pc.amount)/100)*0.0011,2) /*chiles peso*/
		when pc.currency_id = 26 then round(((pc.amount)/100)*0.015,2) /*Dominican peso*/
		when pc.currency_id =3 then (pc.amount)/100 /*euro*/
	else null end) as cost_amount_eur,
		c2.name as cost_currency
from project_cost pc
left join currency c2 on pc.currency_id = c2.id
group by 1) as pc on pc.project_wave_id = concat(ppg.project_id,'/',ppg.id)
left join (select concat(project_id,'/',project_product_group_id) as project_wave_id,
					case when sum(hours) is null then sum(minutes)/60
					else (sum(hours)*60+sum(minutes))/60 
					end as time_log
			from project_time_log
			group by 1) as ptl on ptl.project_wave_id=concat(ppg.project_id,'/',ppg.id)
where p.archived_at is null and p.deleted_at is null /*and ad.invoice_number is null or ad.invoice_number not like '%TEMP%'*/
group by 1,2,3,4,5,6,8, 11, 12;""")
data_export_ext = cur.fetchall()

data_export_ext_df = pd.DataFrame(data_export_ext)
data_export_ext_df.columns = (
'project_id', 'project_wave_id', 'client_managing_company', 'managing_office', 'managing_company', 'project_name',
'client', 'product', 'revenue', 'sales_amount_eur', 'sales_currency', 'currency_code', 'invoice_amount',
'invoice_amount_eur', 'invoice_currency', 'invoice_currency_code', 'cost_amount', 'cost_amount_eur', 'cost_currency',
'time_log', 'invoice_status', 'invoice_number', 'submit_date', 'project_close_date', 'last_sales_date', 'project_date')
data_export_ext_df['revenue'] = pd.to_numeric(data_export_ext_df['revenue']).fillna(0)
data_export_ext_df['sales_amount_eur'] = pd.to_numeric(data_export_ext_df['sales_amount_eur']).fillna(0)
data_export_ext_df['invoice_amount'] = pd.to_numeric(data_export_ext_df['invoice_amount']).fillna(0)
data_export_ext_df['invoice_amount_eur'] = pd.to_numeric(data_export_ext_df['invoice_amount_eur']).fillna(0)
data_export_ext_df['cost_amount'] = pd.to_numeric(data_export_ext_df['cost_amount']).fillna(0)
data_export_ext_df['cost_amount_eur'] = pd.to_numeric(data_export_ext_df['cost_amount_eur']).fillna(0)
data_export_ext_df['time_log'] = pd.to_numeric(data_export_ext_df['time_log']).fillna(0)
data_export_ext_df['submit_date'] = pd.to_datetime(data_export_ext_df['submit_date'], errors='ignore')
data_export_ext_df['project_date'] = pd.to_datetime(data_export_ext_df['project_date'], errors='ignore')
data_export_ext_df['last_sales_date'] = pd.to_datetime(data_export_ext_df['last_sales_date'], errors='ignore')
data_export_ext_df['project_close_date'] = pd.to_datetime(data_export_ext_df['project_close_date'], errors='ignore')

# data_export_ext_df

# adding weeknumbers for differend dates to the table
data_export_ext_df['sales_week_nr'] = data_export_ext_df['last_sales_date'].dt.strftime('%Y%U')
data_export_ext_df['invoice_week_nr'] = data_export_ext_df['submit_date'].dt.strftime('%Y%U')
data_export_ext_df['project_close_week_nr'] = data_export_ext_df['project_close_date'].dt.strftime('%Y%U')
data_export_ext_df['project_week_nr'] = data_export_ext_df['project_date'].dt.strftime('%Y%U')

# merging the data_export with the currency data
df_join = data_export_ext_df.merge(df_cur1, left_on=['sales_week_nr', 'currency_code'],
                                   right_on=['week_nr', 'currency_code'], how='left')
# df_join
# adding euro conversion rate to the data column
# df_join['rate_avg'][df_join.sales_currency == 'Euro'] = 1
df_join.loc[df_join['sales_currency'] == 'Euro', 'rate_avg'] = 1
# calculating the sales value with dynamic currancy
df_join['dc_sales_amount'] = df_join['revenue'] / df_join['rate_avg']
df_join.round(2)
print("df_join gerenated")
# same procedure for invoice data
df_join1 = df_join.merge(df_cur2, left_on=['invoice_week_nr', 'invoice_currency_code'],
                         right_on=['week_nr', 'currency_code'], how='left')
# df_join1['rate_avg_inv'][df_join1.sales_currency == 'Euro'] = 1
df_join1.loc[df_join1['invoice_currency'] == 'Euro', 'rate_avg_inv'] = 1
df_join1['dc_invoice_amount'] = df_join1['invoice_amount'] / df_join1['rate_avg_inv']
df_join1.round(2)
print('df_join1 generated ')
# PUSHING PROJECT TIME DATA TO LIBRARY DATABASE
msql_connector = c['logins'][1]['ds_projects']
sqlEngine = create_engine(msql_connector, pool_recycle=3600)  # creating a connection to the database

dbConnection = sqlEngine.connect()
try:
    frame = df_join1.to_sql('syno_data_export_ext', dbConnection, if_exists='replace', index=False);
except ValueError as vx:  # error handling
    print(vx)

except Exception as ex:
    print(ex)
else:
    print("Data export extended table written to database .")
finally:
    dbConnection.close()  # closing the connection to the database
conn.close()

import sys
from slacker import Slacker

slack = Slacker('xoxb-1483962724294-1731766923767-x1YWHXy7Dejdb38yjB5bKTGO')
message = "Syno internal project reporting datasets have been updated"
slack.chat.post_message('#data-set-update-log', message);
