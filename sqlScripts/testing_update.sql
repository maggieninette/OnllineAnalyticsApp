			CREATE OR REPLACE VIEW old_top_50_products AS 
		            SELECT product_id AS product_id, product_name AS product_name, totalsale AS total
		            FROM top_product_sales 
		            ORDER BY total DESC 
		            LIMIT 50; 
		            
		            UPDATE top_product_sales 
		            SET totalsale = top_product_sales.totalsale + logtable.total
		            FROM 
		                (SELECT product_id, SUM(total) AS total 
		                FROM log 
		                GROUP BY product_id 
		                ) AS logtable 
		                WHERE logtable.product_id = top_product_sales.product_id 
		                AND top_product_sales.product_id = logtable.product_id; 
			
			
            CREATE OR REPLACE VIEW new_top_50_products AS 
            SELECT  product_id AS product_id, product_name AS product_name, totalsale AS total
            FROM top_product_sales 
            ORDER BY total DESC 
            LIMIT 50; 
		
            SELECT * 
            FROM old_top_50_products 
            WHERE product_id NOT IN 
                (SELECT product_id 
                FROM new_top_50_products 
                );
                
