DROP TABLE IF EXISTS log,trigger_test;

CREATE TABLE trigger_test (
    id SERIAL PRIMARY KEY,
    cart_id INTEGER REFERENCES shopping_cart(id) NOT NULL,
    person_id INTEGER REFERENCES person(id) NOT NULL,
    state_name TEXT,
    product_id INTEGER REFERENCES product(id) NOT NULL,
    product_name TEXT,
    total INTEGER
    );  

CREATE TABLE log(
    id SERIAL PRIMARY KEY,
    cart_id INTEGER REFERENCES shopping_cart(id) NOT NULL,
    person_id INTEGER REFERENCES person(id) NOT NULL,
    state_name TEXT,
    product_id INTEGER REFERENCES product(id) NOT NULL,
    product_name TEXT,
    total INTEGER
    ); 
    
CREATE OR REPLACE FUNCTION public.update_log()
	RETURNS TRIGGER
    LANGUAGE 'plpgsql'
    NOT LEAKPROOF
AS $BODY$
BEGIN
	IF NEW.is_purchased = 'true' AND OLD.is_purchased='false' THEN
     
       INSERT INTO log (cart_id,person_id,state_name,product_id,product_name,total)
       SELECT sc.id, ps.id,st.state_name,p.id, p.product_name,(p.price*pc.quantity)
       FROM shopping_cart sc, product p, products_in_cart pc, person ps, state st
       WHERE ps.id=sc.person_id 
       AND sc.id=NEW.id 
       AND pc.cart_id=sc.id 
       AND p.id=pc.product_id   
       AND ps.state_id=st.id;
                                       
    END IF;
    RETURN NULL;
END;
$BODY$;

DROP TRIGGER IF EXISTS after_update_shoppingcart on shopping_cart;
DROP TRIGGER IF EXISTS after_insert_productsincart on products_in_cart;
 
CREATE TRIGGER after_update_shoppingcart
    AFTER UPDATE OF is_purchased 
    ON shopping_cart
    FOR EACH ROW
	EXECUTE PROCEDURE update_log()
    ;
    
    
CREATE OR REPLACE FUNCTION public.update_log_after_insert()
	RETURNS TRIGGER AS $update_log$
BEGIN
       
       INSERT INTO trigger_test (cart_id,person_id,state_name,product_id,product_name,total)
       SELECT sc.id, ps.id,st.state_name,p.id, p.product_name,(p.price*pc.quantity)
       FROM shopping_cart sc, product p, products_in_cart pc, person ps, state st
       WHERE NEW.id=pc.id
       AND NEW.cart_id=sc.id
       AND sc.person_id=ps.id
       AND NEW.product_id=p.id   
       AND ps.state_id=st.id
       AND sc.is_purchased='true';
       
RETURN NEW;
END;
$update_log$ LANGUAGE plpgsql;
 
 

CREATE TRIGGER after_insert_productsincart AFTER INSERT ON products_in_cart
FOR EACH ROW
EXECUTE PROCEDURE update_log_after_insert()
;

    
    
