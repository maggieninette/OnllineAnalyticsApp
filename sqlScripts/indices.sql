CREATE INDEX person_name_index ON person (person_name);
CREATE INDEX shopping_cart_person_id_index ON shopping_cart (person_id);
CREATE INDEX state_name_index ON state (state_name);
CREATE INDEX product_category_id_index ON product (category_id);
CREATE INDEX category_name_index ON category (category_name);

/* Maybe helpful. */
CREATE INDEX shopping_cart_is_purchased_index ON shopping_cart (is_purchased); /* Maybe when most carts not purchased. */
CREATE INDEX person_role_id_index ON person (role_id); /* Maybe when most persons are of one type (i.e. customer/owner). */
CREATE INDEX product_name_index ON product (product_name);
CREATE INDEX products_in_cart_cart_id_index ON products_in_cart (cart_id);
CREATE INDEX role_name_index ON role (role_name);