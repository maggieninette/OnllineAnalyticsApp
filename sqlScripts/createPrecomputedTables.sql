DROP TABLE IF EXISTS TopProductSales;

CREATE TABLE TopProductSales (
    product_id INTEGER,
    product_name TEXT,
    totalsale INTEGER    
    );


CREATE TABLE OldTop50Products (
    product_id INTEGER,
    product_name TEXT
    
    );


INSERT INTO TopProductSales(
SELECT p.id as product_id, p.product_name as product_name, COALESCE(totalsale, 0) as total 
FROM product p LEFT OUTER JOIN 
			(SELECT product_id, SUM(totalsale) AS totalsale 
             FROM (
                   SELECT prod.product_name AS product_name, p.person_name AS person_name, prod.id AS product_id, (prod.price * pc.quantity) AS totalsale 
                    FROM person p, product prod, shopping_cart sc, products_in_cart pc
                    WHERE p.id = sc.person_id 
                    AND sc.id = pc.cart_id 
                    AND sc.is_purchased = 'true' 
                    AND pc.product_id = prod.id)
             		AS customerpurchases 
              GROUP BY product_id) 
              AS salesmade 
            
            ON p.id = product_id
    );
 
DROP TABLE IF EXISTS TopStateSales; 
 
CREATE TABLE TopStateSales (
    state_id INTEGER,
    state_name TEXT,
    totalsale INTEGER    
    );
    
INSERT INTO TopStateSales (
SELECT allstates.id as state_id, allstates.state_name as state_name,COALESCE(total,0) as total_sale
FROM
   
    (SELECT id as id, state_name as state_name
    FROM state
     ) AS allstates
	
LEFT OUTER JOIN

    (SELECT st.state_name as state_name, SUM(pc.quantity*pc.price) as total
    FROM shopping_cart sc2, products_in_cart pc, product p2, person p, state st
    WHERE pc.cart_id=sc2.id AND pc.product_id=p2.id
         AND sc2.person_id=p.id AND p.state_id=st.id
    GROUP BY st.state_name
     ) AS purchasesperstate
     ON allstates.state_name=purchasesperstate.state_name
ORDER BY total_sale desc     
    
    
    );
    
DROP TABLE IF EXISTS cellValues;    
    
CREATE TABLE cellValues(
    state_name TEXT,
    product_name TEXT,
    total INTEGER
    );
    
INSERT INTO cellValues(
SELECT allproductsandstates.state_name as state_name, allproductsandstates.product_name as product_name, 
    	COALESCE(total,0) as total
FROM

	(SELECT state_name as state_name, product_name as product_name
    FROM product, state ) AS allproductsandstates
    
    LEFT OUTER JOIN
   
   	(SELECT st.state_name as state_name, p2.product_name as product_name, sum(pc.quantity*pc.price) as total
    FROM shopping_cart sc2, products_in_cart pc, product p2, person p, state st
    WHERE pc.cart_id=sc2.id AND pc.product_id=p2.id
         AND sc2.person_id=p.id AND p.state_id=st.id
    GROUP BY (st.state_name,p2.product_name)
    ORDER BY p2.product_name   
    ) AS salesmade
    
    ON allproductsandstates.state_name= salesmade.state_name
       AND allproductsandstates.product_name= salesmade.product_name 
    
    );
    
    
    