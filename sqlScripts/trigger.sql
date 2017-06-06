DROP TABLE IF EXISTS log;

CREATE TABLE log(
    id SERIAL PRIMARY KEY,
    cart_id INTEGER REFERENCES shopping_cart(id) NOT NULL,
    person_id INTEGER REFERENCES person(id) NOT NULL,
    state_name TEXT,
    product_id INTEGER REFERENCES product(id) NOT NULL,
    total INTEGER
    ); 
    
CREATE OR REPLACE FUNCTION public.update_log()
	RETURNS TRIGGER
    LANGUAGE 'plpgsql'
    NOT LEAKPROOF
AS $BODY$
BEGIN
	IF NEW.is_purchased = 'true' AND OLD.is_purchased='false' THEN
     
       INSERT INTO log (cart_id,person_id,state_name,product_id,total)
       SELECT sc.id, ps.id,st.state_name,p.id, (p.price*pc.quantity)
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
    
CREATE TRIGGER after_update_shoppingcart
    AFTER UPDATE OF is_purchased 
    ON shopping_cart
    FOR EACH ROW
	EXECUTE PROCEDURE update_log()
    ;
