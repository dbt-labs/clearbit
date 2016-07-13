### to do: ###

- connect to redshift, get domains that have not had lookups in the recent past and look them up
  - make sure to only lookup domains that have ltv > 0







```
with users as (

  select *
  from uvwarehouse.v_users

)

select email, regexp_substr(email, '[^@]*$') as email_domain
from users
limit 100
```
