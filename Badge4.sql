select util_db.public.grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123 ) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 

create or replace table util_db.public.my_data_types
(
  my_number number
, my_text varchar(10)
, my_bool boolean
, my_float float
, my_date date
, my_timestamp timestamp_tz
, my_variant variant
, my_array array
, my_object object
, my_geography geography
, my_geometry geometry
, my_vector vector(int,16)
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DLKW01' as step
  ,( select count(*)  
      from ZENAS_ATHLEISURE_DB.INFORMATION_SCHEMA.STAGES 
      where stage_schema = 'PRODUCTS'
      and 
      (stage_type = 'Internal Named' 
      and stage_name = ('PRODUCT_METADATA'))
      or stage_name = ('SWEATSUITS')
   ) as actual
, 2 as expected
, 'Zena stages look good' as description
); 

list @product_metadata;

select $1
from @product_metadata/product_coordination_suggestions.txt; 

create file format zmd_file_format_1
RECORD_DELIMITER = '^';

select $1
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_1);

create file format zmd_file_format_2
FIELD_DELIMITER = '^';

select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_2);

create or replace file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^';

select $1, $2
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

create or replace file format zmd_file_format_1
RECORD_DELIMITER = ';'
TRIM_SPACE = True;

select replace($1, chr(13)||chr(10)) as sizes_available
from @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 )
where sizes_available <> '';

create or replace file format zmd_file_format_2
FIELD_DELIMITER = '|'
RECORD_DELIMITER = ';'
TRIM_SPACE = True ;

select $1, $2, $3
from @product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2 );

select replace($1, chr(13)||chr(10)) as PRODUCT_CODE, $2 as HEADBAND_DESCRIPTION, $3 as WRISTBAND_DESCRIPTION
from @product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2 );

create or replace view zenas_athleisure_db.products.sweatsuit_sizes as (select replace($1, chr(13)||chr(10)) as sizes_available
from @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 )where sizes_available <> '');

select * from zenas_athleisure_db.products.sweatsuit_sizes;


select $1, $2
from @product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2);

create or replace view zenas_athleisure_db.products.sweatband_product_line as (select replace($1, chr(13)||chr(10)) as PRODUCT_CODE, $2 as HEADBAND_DESCRIPTION, $3 as WRISTBAND_DESCRIPTION
from @product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2));

select * from zenas_athleisure_db.products.sweatband_product_line;


select $1, $2
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

create or replace view zenas_athleisure_db.products.SWEATBAND_COORDINATION as (select $1 as PRODUCT_CODE, $2 as HAS_MATCHING_SWEATSUIT
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3));

select * from zenas_athleisure_db.products.SWEATBAND_COORDINATION;



select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
   'DLKW02' as step
   ,( select sum(tally) from
        ( select count(*) as tally
        from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATBAND_PRODUCT_LINE
        where length(product_code) > 7 
        union
        select count(*) as tally
        from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUIT_SIZES
        where LEFT(sizes_available,2) = char(13)||char(10))     
     ) as actual
   ,0 as expected
   ,'Leave data where it lands.' as description
); 

select product_code, has_matching_sweatsuit
from zenas_athleisure_db.products.sweatband_coordination;

select product_code, headband_description, wristband_description
from zenas_athleisure_db.products.sweatband_product_line;

select sizes_available
from zenas_athleisure_db.products.sweatsuit_sizes;


list @ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS;

select $1 from @sweatsuits;

select $1
from @sweatsuits/purple_sweatsuit.png;

select metadata$filename, metadata$file_row_number
from @sweatsuits/purple_sweatsuit.png;

select METADATA$FILENAME , count(*) from @ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS group by METADATA$FILENAME ;

select * 
from directory(@sweatsuits);

select REPLACE(relative_path, '_', ' ') as no_underscores_filename
, REPLACE(no_underscores_filename, '.png') as just_words_filename
, INITCAP(just_words_filename) as product_name
from directory(@sweatsuits);

select INITCAP(REPLACE(REPLACE(relative_path, '_', ' '), '.png')) as product_name
from directory(@sweatsuits);

--create an internal table for some sweatsuit info
create or replace table zenas_athleisure_db.products.sweatsuits (
	color_or_style varchar(25),
	file_name varchar(50),
	price number(5,2)
);

--fill the new table with some data
insert into  zenas_athleisure_db.products.sweatsuits 
          (color_or_style, file_name, price)
values
 ('Burgundy', 'burgundy_sweatsuit.png',65)
,('Charcoal Grey', 'charcoal_grey_sweatsuit.png',65)
,('Forest Green', 'forest_green_sweatsuit.png',64)
,('Navy Blue', 'navy_blue_sweatsuit.png',65)
,('Orange', 'orange_sweatsuit.png',65)
,('Pink', 'pink_sweatsuit.png',63)
,('Purple', 'purple_sweatsuit.png',64)
,('Red', 'red_sweatsuit.png',68)
,('Royal Blue',	'royal_blue_sweatsuit.png',65)
,('Yellow', 'yellow_sweatsuit.png',67);

select * from directory(@sweatsuits);

select * from sweatsuits;

select * from directory(@product_metadata);

create or replace view PRODUCT_LIST as (select  INITCAP(REPLACE(REPLACE(relative_path, '_', ' '), '.png')) as product_name , file_name, color_or_style, price, file_url from sweatsuits s join directory(@sweatsuits) d on s.file_name = d.relative_path  );

select * from product_list;


-- CREATE OR REPLACE VIEW PRODUCT_LIST AS
-- SELECT 
--     INITCAP(REPLACE(REPLACE(relative_path, '_', ' '), '.png', '')) AS PRODUCT_NAME,
--     METADATA$FILENAME AS FILE_NAME,
--     COLOR_OR_STYLE,
--     PRICE,
--     FILE_URL
-- FROM 
--     sweatsuits;

select * 
from product_list p
cross join sweatsuit_sizes;

create or replace view catalog as select * 
from product_list p
cross join sweatsuit_sizes;


select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DLKW03' as step
 ,( select count(*) from ZENAS_ATHLEISURE_DB.PRODUCTS.CATALOG) as actual
 ,180 as expected
 ,'Cross-joined view exists' as description
); 


-- Add a table to map the sweatsuits to the sweat band sets
create table zenas_athleisure_db.products.upsell_mapping
(
sweatsuit_color_or_style varchar(25)
,upsell_product_code varchar(10)
);

--populate the upsell table
insert into zenas_athleisure_db.products.upsell_mapping
(
sweatsuit_color_or_style
,upsell_product_code 
)
VALUES
('Charcoal Grey','SWT_GRY')
,('Forest Green','SWT_FGN')
,('Orange','SWT_ORG')
,('Pink', 'SWT_PNK')
,('Red','SWT_RED')
,('Yellow', 'SWT_YLW');

-- Zena needs a single view she can query for her website prototype
create view catalog_for_website as 
select color_or_style
,price
,file_name
, get_presigned_url(@sweatsuits, file_name, 3600) as file_url
,size_list
,coalesce('Consider: ' ||  headband_description || ' & ' || wristband_description, 'Consider: White, Black or Grey Sweat Accessories')  as upsell_product_desc
from
(   select color_or_style, price, file_name
    ,listagg(sizes_available, ' | ') within group (order by sizes_available) as size_list
    from catalog
    group by color_or_style, price, file_name
) c
left join upsell_mapping u
on u.sweatsuit_color_or_style = c.color_or_style;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DLKW04' as step
 ,( select count(*) 
  from zenas_athleisure_db.products.catalog_for_website 
  where upsell_product_desc not like '%e, Bl%') as actual
 ,6 as expected
 ,'Relentlessly resourceful' as description
); 
left join sweatband_coordination sc
on sc.product_code = u.upsell_product_code
left join sweatband_product_line spl
on spl.product_code = sc.product_code;
