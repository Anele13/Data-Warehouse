-- Se configura dblink en la nueva Base de datos,
CREATE EXTENSION dblink;

-- Tablas de equivalencia
CREATE TABLE equivalenciaClientes(
	nro_cliente integer,
	cod_cliente integer,
	unificado serial,
	CONSTRAINT "PK_Equivalencia" PRIMARY KEY (unificado)
)

CREATE TABLE equivalenciaProducto(
	nro_producto integer,
	cod_producto integer,
	unificado serial,
	CONSTRAINT "PK_EquivalenciaP" PRIMARY KEY (unificado)
)

-- insert en equivalencia producto desde patsur1 y patsur2 con primitivas dblink()
insert into equivalenciaProducto (nro_producto)
	(SELECT * FROM dblink('myconn1','SELECT nro_producto tipo FROM producto') AS t(nro_producto integer))

insert into equivalenciaProducto (cod_producto)
	(SELECT * FROM dblink('myconn2','SELECT cod_producto tipo FROM producto') AS t(cod_producto integer))

select * from Equivalenciaproducto

insert into equivalenciaClientes (nro_cliente)
	(SELECT * FROM dblink('myconn1','SELECT nro_cliente FROM cliente') AS t(nro_cliente integer))




-- cracion de tablas del datawarehouse

CREATE TABLE Cliente(
	id_cliente integer UNIQUE, 
	nombre varchar(30),
	id_tipo varchar(30),
	CONSTRAINT "PK_Clientes" PRIMARY KEY (id_cliente)
	)

CREATE TABLE tipoCliente(
	id_tipo serial,
	descripcion varchar(30),
	CONSTRAINT "PK_	TipoCliente" PRIMARY KEY(id_tipo)
	)

CREATE TABLE Producto(
	id_producto integer,
	nombre varchar(30),
	id_categoria integer,
	cod_subcategoria integer,
	CONSTRAINT "PK_Producto" PRIMARY KEY (id_producto),
	CONSTRAINT "FK_Categoria" FOREIGN KEY (id_categoria) REFERENCES Categoria
	)

CREATE TABLE Categoria(
	id_categoria serial,
	descripcion varchar(40),
	id_subcategoria integer,
	CONSTRAINT "PK_Categoria" PRIMARY KEY (id_categoria)
	)

CREATE TABLE medioPago(
	id_medio_pago serial,
	descripcion varchar(30),
	CONSTRAINT "PK_medioPago" PRIMARY KEY (id_medio_pago)
	)

CREATE TABLE Region(
	id_region integer UNIQUE,
	descripcion varchar(30),
	CONSTRAINT "PK_Region" PRIMARY KEY (id_region)
	);
	
CREATE TABLE Provincia(
	id_provincia integer UNIQUE,
	descripción varchar(30),
	id_region integer,
	CONSTRAINT "PK_Provincia" PRIMARY KEY (id_provincia),
	CONSTRAINT "FK_Region" FOREIGN KEY (id_region) REFERENCES Region
	);
	
CREATE TABLE Ciudad(
	id_ciudad integer UNIQUE,
	descripción varchar(30),
	id_provincia integer,
	CONSTRAINT "PK_Ciudad" PRIMARY KEY (id_ciudad),
	CONSTRAINT "FK_provincia" FOREIGN KEY (id_provincia) REFERENCES Provincia
	);

CREATE TABLE distribucionGeografica(
	id_sucursal serial,
	descripcion varchar(30),
	id_ciudad integer,
	CONSTRAINT "PK_DistGeog" PRIMARY KEY (id_sucursal),
	CONSTRAINT "FK_Ciudad"	FOREIGN KEY (id_ciudad) REFERENCES Ciudad
	);

CREATE TABLE Tiempo(
	id_tiempo serial,
	mes integer,
	trimestre integer,
	año integer,
	CONSTRAINT "PK_Tiempo" PRIMARY KEY (id_tiempo)
)

-- insert de region
INSERT INTO region VALUES(1,'region del norte');
INSERT INTO region VALUES(2,'region del sur');
INSERT INTO region VALUES(3,'region del este');
INSERT INTO region VALUES(4,'region del oeste');

-- insert de provincia
INSERT INTO provincia VALUES(1,'gaiman',4);
INSERT INTO provincia VALUES(2,'rawson',4);
INSERT INTO provincia VALUES(3,'escalante',2);
INSERT INTO provincia VALUES(4,'esquel',3);
INSERT INTO provincia VALUES(5,'rio senguer',3);
INSERT INTO provincia VALUES(6,'sarmiento',2);
INSERT INTO provincia VALUES(7,'viedma',4);


-- que hice en provincia??? no son provincias

-- insert de ciudad
INSERT INTO ciudad VALUES(1,'gaiman',1);
INSERT INTO ciudad VALUES(2,'trelew',2);
INSERT INTO ciudad VALUES(3,'rawson',2);
INSERT INTO ciudad VALUES(4,'comodoro rivadavia',3);
INSERT INTO ciudad VALUES(5,'futaleufu',4);
INSERT INTO ciudad VALUES(6,'madryn',7);

CREATE TABLE Venta(
	fecha date,
	id_factura integer,
	id_tiempo integer,
	id_cliente integer,
	id_producto integer,
	id_sucursal integer,
	id_medio_pago integer,
	montoVendido integer,
	cantidadVendida integer,	
	CONSTRAINT "PK_Venta" PRIMARY KEY (fecha,id_factura,id_cliente,id_producto,id_sucursal,id_medio_pago),
	CONSTRAINT "FK_Cliente" FOREIGN KEY (id_cliente) REFERENCES cliente,
	CONSTRAINT "FK_Producto" FOREIGN KEY (id_producto) REFERENCES Producto,
	CONSTRAINT "FK_Sucursal" FOREIGN KEY (id_sucursal) REFERENCES distribucionGeografica,
	CONSTRAINT "FK_MedioPago" FOREIGN KEY (id_medio_pago) REFERENCES medioPago,
	CONSTRAINT "FK_Tiempo" FOREIGN KEY(id_tiempo) REFERENCES tiempo
	)

-- funcion para la carga de datos en el datawarehouse
CREATE OR REPLACE FUNCTION llenarDW (mes integer, año integer, sucursal varchar) RETURNS void AS $$
DECLARE
	aux RECORD;
	aux2 RECORD;
BEGIN
	IF sucursal='trelew' THEN 
		PERFORM dblink_connect('myconn1', 'dbname='||sucursal||' user=db user=dblinktest password=prueba');
		PERFORM dblink_open('myconn1','cursor1', 'select fecha_venta, nro_factura, nro_cliente, forma_pago from venta where fecha_venta between '''||año||'-'||mes||'-01'' and '''||año||'-'||mes||'-15''');
		LOOP
		   SELECT * FROM dblink_fetch('myconn1','cursor1', 1) AS d(fecha date, idfactura integer, idcliente integer, formapago varchar(30)) INTO aux;
		   IF NOT FOUND THEN
			exit;
		   ELSE
			PERFORM dblink_open('myconn1','cursor2', 'select nro_producto, unidad, precio from DetalleVenta where nro_factura='||aux.idfactura);
			LOOP
			   SELECT * FROM dblink_fetch('myconn1','cursor2',1) as e(nro_producto integer, unidad integer, precio float) INTO aux2;
			   IF NOT FOUND THEN
				exit;
			   ELSE
			       INSERT INTO venta VALUES (aux.fecha,
							 aux.idfactura,
							 (SELECT verificar_tiempo(aux.fecha)),
							 (SELECT existe_cliente(aux.idcliente, sucursal)),
							 (SELECT existe_producto(aux2.nro_producto, sucursal)),
							 (SELECT verificar_sucursal(sucursal)),
							 (SELECT verificar_medio_pago(aux.formapago)),
							 (aux2.unidad*aux2.precio),
							 (aux2.unidad)
							 );
			   END IF;
			END LOOP;
			PERFORM dblink_close('myconn1','cursor2');
		   END IF;
		END LOOP;
	ELSE
		PERFORM dblink_connect('myconn1', 'dbname='||sucursal||' user=db user=dblinktest password=prueba');
		PERFORM dblink_open('myconn1','cursor1', 'select fecha_venta, id_factura, cod_cliente, descripcion from venta V, medioPago MP where fecha_venta between '''||año||'-'||mes||'-01'' and '''||año||'-'||mes||'-15'' and V.cod_medio_pago = MP.cod_medio_pago');
		LOOP
		   SELECT * FROM dblink_fetch('myconn1','cursor1', 1) AS d(fecha date, idfactura integer, id_cliente integer, formapago varchar) INTO aux;
		   IF NOT FOUND THEN
			exit;
		   ELSE
			PERFORM dblink_open('myconn1','cursor2', 'select cod_producto, unidad, precio from DetalleVenta where id_factura='||aux.idfactura);
			LOOP
			   SELECT * FROM dblink_fetch('myconn1','cursor2',1) as e(cod_producto integer, unidad integer, precio float) INTO aux2;
			   IF NOT FOUND THEN
				exit;
			   ELSE
			       INSERT INTO venta VALUES (aux.fecha,
							 aux.idfactura,
							 (SELECT verificar_tiempo(aux.fecha)),
							 (SELECT existe_cliente(aux.id_cliente, sucursal)),
							 (SELECT existe_producto(aux2.cod_producto, sucursal)),
							 (SELECT verificar_sucursal(sucursal)),
							 (SELECT verificar_medio_pago(aux.formapago)),
							 (aux2.unidad*aux2.precio),
							 (aux2.unidad)
							 );
			   END IF;
			END LOOP;
			PERFORM dblink_close('myconn1','cursor2');
		   END IF;
		END LOOP;
	END IF;
	PERFORM dblink_close('myconn1','cursor1');
	PERFORM dblink_disconnect('myconn1');
END;
$$ LANGUAGE plpgsql;


select dblink_disconnect('myconn1');

select llenarDW (3, 2009, 'trelew') -- febrero del 2009 del sistema viejo(trelew), los nuevos se llaman madryn y comodoro


-- estos estan para ver que se carga en cada ejecucion del ETL, para ver que queda guardado
select * from venta
select * from cliente
select * from producto
select * from tipocliente
select * from categoria
select * from mediopago
select * from tiempo
select * from distribucionGeografica


-- por si se necesita borrar datos.. 
delete from distribucionGeografica cascade;
delete from mediopago cascade;
delete from categoria cascade;
delete from tipocliente cascade;
delete from tiempo cascade;
delete from producto cascade;
delete from cliente cascade;
delete from venta cascade;


-- verifica que exista un producto en el DW, si existe NO lo agrega de nuevo!!!
CREATE OR REPLACE FUNCTION existe_producto(nro_prod integer, sucursal varchar) RETURNS integer AS $$
DECLARE
	nuevo_id_producto integer;
	nombreProd varchar;
	DESCcategoria varchar;
	categoriaDW integer;
	subcategoria integer;
BEGIN
	IF sucursal='trelew' THEN
		nuevo_id_producto:= (SELECT unificado FROM equivalenciaProducto WHERE nro_producto = nro_prod);
		IF nuevo_id_producto NOT IN(SELECT id_producto FROM producto WHERE id_producto = nuevo_id_producto) THEN
			nombreProd:= (SELECT nombre FROM dblink('myconn1','SELECT nombre FROM "producto" where nro_producto='||nro_prod) AS t(nombre varchar(30)));
			DESCcategoria:=(SELECT descripcionCat FROM dblink('myconn1','SELECT C.descripción  FROM producto P, categoria C where nro_producto='||nro_prod||' and P.nro_categoria=C.nro_categoria') AS t(descripcionCat varchar));
			categoriaDW:= (SELECT verificar_categoria(DESCcategoria));
			subcategoria:= 1;
			INSERT INTO producto VALUES(nuevo_id_producto, nombreProd, categoriaDW, subcategoria);
		END IF;
	ELSE
		nuevo_id_producto:= (SELECT unificado FROM equivalenciaProducto WHERE nro_producto = nro_prod);
		IF nuevo_id_producto NOT IN(SELECT id_producto FROM producto WHERE id_producto = nuevo_id_producto) THEN
			nombreProd:= (SELECT nombre FROM dblink('myconn1','SELECT nombre FROM "producto" where cod_producto='||nro_prod) AS t(nombre varchar(30)));
			DESCcategoria:=(SELECT descripcionCat FROM dblink('myconn1','SELECT C.descripción FROM producto P, categoria C where cod_producto='||nro_prod||' and P.cod_categoria=C.cod_categoria') AS t(descripcionCat varchar));
			categoriaDW:= (SELECT verificar_categoria(DESCcategoria));
			subcategoria:= 1;
			INSERT INTO producto VALUES(nuevo_id_producto, nombreProd, categoriaDW, subcategoria);
		END IF;
	END IF;
	RETURN nuevo_id_producto;
END;
$$ LANGUAGE plpgsql;


-- verifica que exista un cliente en el DW, si existe NO lo agrega de nuevo!!
CREATE OR REPLACE FUNCTION existe_cliente(idcliente integer, sucursal varchar) RETURNS integer AS $$
DECLARE
	nuevo_id_cliente integer;
	nombreC varchar;
	id_tipo integer;
	descripcionTipo varchar(30);
BEGIN
	IF sucursal='trelew' THEN
		nuevo_id_cliente:= (SELECT unificado FROM equivalenciaClientes WHERE nro_cliente = idcliente);-- CUAL ES EL ID EN LA TABLA EQUIVALENCIA
		IF nuevo_id_cliente NOT IN(SELECT id_cliente FROM cliente WHERE id_cliente = nuevo_id_cliente) THEN -- PREGUNTO SI NO ESTA AGREGADO ESE CLIENTE CON CAMPO UNIFICADO EN LA TABLA CLIENTE DEL DW
			nombreC:= (SELECT nombre FROM dblink('myconn1','SELECT nombre FROM "cliente" where nro_cliente='||idcliente) AS t(nombre varchar(30)));
			descripcionTipo:= (SELECT tipo FROM dblink('myconn1','SELECT tipo FROM "cliente" where nro_cliente='||idcliente) AS t(tipo varchar(30)));
			id_tipo:= (SELECT verificar_Tipo_Cliente(descripcionTipo));
			INSERT INTO cliente VALUES(nuevo_id_cliente, nombreC, id_tipo);
		END IF;
	ELSE
		nuevo_id_cliente:= (SELECT unificado FROM equivalenciaClientes WHERE cod_cliente = idcliente);-- CUAL ES EL ID EN LA TABLA EQUIVALENCIA
		IF nuevo_id_cliente NOT IN(SELECT id_cliente FROM cliente WHERE id_cliente = nuevo_id_cliente) THEN -- PREGUNTO SI NO ESTA AGREGADO ESE CLIENTE CON CAMPO UNIFICADO EN LA TABLA CLIENTE DEL DW
			nombreC:= (SELECT nombre FROM dblink('myconn1','SELECT nombre FROM "cliente" where cod_cliente='||idcliente) AS t(nombre varchar));
			descripcionTipo:= (SELECT descripcion FROM dblink('myconn1','SELECT descripción FROM "cliente", tipocliente where cod_cliente='||idcliente||' and tipo=cod_tipo') AS t(descripcion varchar));
			id_tipo:= (SELECT verificar_Tipo_Cliente(descripcionTipo));
			INSERT INTO cliente VALUES(nuevo_id_cliente, nombreC, id_tipo);
		END IF;
	END IF;
	RETURN nuevo_id_cliente;
END;
$$ LANGUAGE plpgsql;


-- verifica si existe el tipo de cliente, si existe NO lo carga de nuevo
CREATE OR REPLACE FUNCTION verificar_Tipo_Cliente(descripcion_tipo varchar) RETURNS integer AS $$
BEGIN
	IF descripcion_tipo NOT IN(SELECT descripcion FROM tipoCliente) THEN
		INSERT INTO tipoCliente(descripcion) VALUES(descripcion_tipo);
	END IF;
	RETURN (SELECT id_tipo FROM tipoCliente WHERE descripcion= descripcion_tipo);
END;
$$ LANGUAGE plpgsql;


-- verifica si existe la categoria del producto
CREATE OR REPLACE FUNCTION verificar_categoria(descrip varchar) RETURNS integer AS $$
BEGIN
	IF descrip NOT IN(SELECT descripcion FROM categoria) THEN
		INSERT INTO categoria(descripcion) VALUES(descrip);
	END IF;
	RETURN (SELECT id_categoria FROM categoria WHERE descripcion= descrip);
END;
$$ LANGUAGE plpgsql;


-- verifica que exista la forma de pago, si no existe la agrega
CREATE OR REPLACE FUNCTION verificar_medio_pago(formapago varchar) RETURNS integer AS $$
BEGIN
	IF formapago NOT IN(SELECT descripcion FROM medioPago) THEN
		INSERT INTO medioPago(descripcion) VALUES(formapago);
	END IF;
	RETURN (SELECT id_medio_pago FROM medioPago WHERE descripcion= formapago);
END;
$$ LANGUAGE plpgsql;


-- tabla de tiempo,
CREATE OR REPLACE FUNCTION verificar_tiempo(fecha date)RETURNS integer AS $$
DECLARE
	aux_año integer:=(SELECT date_part('Year',fecha));
	aux_mes integer:=(SELECT date_part('Month',fecha));
	trimestre integer:=(SELECT date_part('Quarter',fecha));
BEGIN
	IF NOT EXISTS (SELECT * from tiempo WHERE año=aux_año and mes=aux_mes) THEN
		INSERT INTO tiempo(año, mes, trimestre) VALUES(aux_año,aux_mes,trimestre);
	END IF;
	RETURN (SELECT id_tiempo from tiempo WHERE año=aux_año and mes=aux_mes);
END;
$$ LANGUAGE plpgsql;


-- verificar la sucursal de la que cargo datos
CREATE OR REPLACE FUNCTION verificar_sucursal(sucursal varchar)RETURNS integer AS $$
BEGIN
	IF NOT EXISTS (SELECT descripcion from distribucionGeografica WHERE descripcion=sucursal) THEN
		INSERT INTO distribucionGeografica(descripcion,id_ciudad) VALUES(sucursal,(SELECT id_ciudad FROM ciudad WHERE descripción=sucursal));
	END IF;
	RETURN (SELECT id_sucursal FROM distribucionGeografica WHERE descripcion = sucursal);
END;
$$ LANGUAGE plpgsql;


-- distintos criterios de agrupamiento NO ESTAN TODOS COMPLETOS!!

-- ventas por cliente
select V.id_cliente, C.nombre, count(*) from venta V, cliente C where v.id_cliente = c.id_cliente
group by V.id_cliente, C.nombre


-- ventas por sucursal
select V.id_sucursal, D.descripcion, count(*) from venta V, distribucionGeografica D where V.id_sucursal = D.id_sucursal
group by V.id_sucursal, D.id_sucursal


-- clientes que generan mayores ingresos
select C.nombre, count(*), sum(V.montoVendido), rank() over (order by sum (V.montoVendido)) as suma from cliente C, venta V where v.id_cliente = c.id_cliente
group by C.id_cliente order by suma