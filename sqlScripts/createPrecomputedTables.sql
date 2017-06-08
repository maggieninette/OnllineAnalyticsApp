DROP TABLE IF EXISTS top_product_sales CASCADE;
DROP TABLE IF EXISTS new_top_50_products CASCADE;
DROP TABLE IF EXISTS old_top_50_products CASCADE;

CREATE TABLE top_product_sales_filtered(
  product_id BIGSERIAL,
  product_name TEXT,
  category_name TEXT,
  totalsale BIGSERIAL
);
CREATE TABLE top_state_sales_filtered(
  state_id BIGSERIAL,
  state_name TEXT,
  category_name TEXT,
  totalsale BIGSERIAL
);

CREATE TABLE precomputed_state_topk_filtered(
  state_name TEXT,
  product_name TEXT,
  total BIGSERIAL
);



CREATE TABLE new_top_50_products (
  product_id BIGSERIAL,
  product_name TEXT,
  totalsale BIGSERIAL
    );

CREATE TABLE old_top_50_products (
  product_id BIGSERIAL,
  product_name TEXT,
  totalsale BIGSERIAL
    );


CREATE TABLE top_product_sales(
  product_id BIGSERIAL,
  product_name TEXT,
  totalsale BIGSERIAL
);

INSERT INTO top_product_sales(
  SELECT p.id as product_id, p.product_name AS product_name, COALESCE(totalsale, 0) AS total
  FROM product p LEFT OUTER JOIN
    (SELECT product_id, SUM(totalsale) AS totalsale
     FROM
       (SELECT prod.product_name AS product_name, p.person_name AS person_name, prod.id AS product_id, (prod.price * pc.quantity) AS totalsale
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


DROP TABLE IF EXISTS top_state_sales CASCADE;

CREATE TABLE top_state_sales(
  state_id BIGSERIAL,
  state_name TEXT,
  totalsale BIGSERIAL
);

INSERT INTO top_state_sales(
  SELECT allstates.id AS state_id, allstates.state_name AS state_name, COALESCE(total,0) AS total_sale
  FROM
    (SELECT id as id, state_name as state_name FROM state) AS allstates
    LEFT OUTER JOIN
    (SELECT st.state_name AS state_name, SUM(pc.quantity * pc.price) AS total
    FROM shopping_cart sc2, products_in_cart pc, product p2, person p, state st
    WHERE pc.cart_id = sc2.id
    AND pc.product_id = p2.id
    AND sc2.person_id = p.id
    AND p.state_id = st.id
    GROUP BY st.state_name
    ) AS purchasesperstate
    ON allstates.state_name = purchasesperstate.state_name
  ORDER BY total_sale desc
);

DROP TABLE IF EXISTS precomputed_state_topk CASCADE;

CREATE TABLE precomputed_state_topk(
  state_name TEXT,
  product_name TEXT,
  total BIGSERIAL
);

INSERT INTO precomputed_state_topk(
  SELECT allproductsandstates.state_name AS state_name, allproductsandstates.product_name AS product_name, COALESCE(total,0) AS total
  FROM
    (SELECT state_name AS state_name, product_name AS product_name
    FROM product, state) AS allproductsandstates
    LEFT OUTER JOIN
      (SELECT st.state_name AS state_name, p2.product_name AS product_name, SUM(pc.quantity * pc.price) AS total
      FROM shopping_cart sc2, products_in_cart pc, product p2, person p, state st
      WHERE pc.cart_id = sc2.id
      AND pc.product_id = p2.id
      AND sc2.person_id = p.id
      AND p.state_id = st.id
      GROUP BY (st.state_name,p2.product_name)
      ORDER BY p2.product_name
    ) AS salesmade
    ON allproductsandstates.state_name = salesmade.state_name
    AND allproductsandstates.product_name = salesmade.product_name
);