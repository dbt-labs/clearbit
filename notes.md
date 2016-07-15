### to do: ###

- connect to redshift, get domains that have not had lookups in the recent past and look them up
  - make sure to only lookup domains that have ltv > 0







```
with users as (

  select *
  from uvwarehouse.v_users

), subdomains as (

  select * from uvwarehouse.v_subdomains

), transactions as (

  select * from dbt_jthandy.transactions_typed

)

select distinct regexp_substr(email, '[^@]*$') as email_domain
from users
  inner join subdomains on users.subdomain_id = subdomains.id
  inner join transactions on subdomains.account_id = transactions.account_id
where state = 'active'
group by 1
having sum(transactions.total) > 0
```
