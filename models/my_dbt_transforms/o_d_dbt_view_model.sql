SELECT
	j.*,
	d.trip_date,
	d.trip_day,
	d.trip_time,
	d.driver_id

FROM
	(
	 SELECT 
	 	c.cus_id,
	 	c.cus_name,
	 	c.cus_address,
	 	o.order_id AS order_id,
	 	o.order_date,
	 	o.order_day,
	 	o.order_time,
	 	o.order_quantity_litres_,
	 	o.order_cost

	FROM
	    {{ source('sheets_tran', 'data_customer') }} AS c,
	    {{ source('sheets_tran', 'data_order') }} AS o  
	
	WHERE
		c.cus_id = o.cus_id
    ) AS j,
 	{{ source('sheets_tran', 'data_delivery') }} AS d	

WHERE
	j.order_id = d.trip_id

ORDER BY
    j.order_date,
    j.order_time;