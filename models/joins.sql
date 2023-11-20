with
    
    prod as (
        select 
            ct.category_name,
            sp.company_name as suppliers,
            pd.product_name, 
            pd.unit_price,
            pd.product_id
            
        from {{source('sources', 'products')}} pd
        left join {{source('sources', 'suppliers')}} sp ON pd.supplier_id = sp.supplier_id
        left join {{source('sources', 'categories')}} as ct ON pd.category_id = ct.category_id
    ),

    orddetail as (
        select
            pd.*,
            od.order_id,
            od.quantity,
            od.discount
        from {{ref('orderdetails')}} od
        left join prod pd on od.product_id = pd.product_id
    ), 

    ordrs as (
        select 
            ord.order_id, 
            ord.order_date, 
            cs.company_name as customer, 
            em.name as employee,
            em.age, 
            em.lengthofservice
        from {{source('sources', 'orders')}} ord
        left join {{ref('customers')}} cs ON ord.customer_id = cs.customer_id
        left join {{ref('employees')}} em on ord.employee_id = em.employee_id
        left join {{source('sources', 'shippers')}} sh on ord.ship_via = sh.shipper_id
    ), 

    final_join as (
        select 
            od.*,
            odd.order_date,
            odd.customer,
            odd.employee,
            odd.age,
            odd.lengthofservice
        from orddetail od
        join ordrs odd on od.order_id = odd.order_id
    )

select * from final_join