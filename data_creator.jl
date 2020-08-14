using DataFrames
using CSV
using Dates
using Base.Iterators
using Random

Random.seed!(123);

joint_pct = 0.15
smoke_pct = 0.2
n_policies = 1500
retention_limit = 1e6
retention_slippage_rate = 0.1 # how many policies above retention_limit don't have full excess ceded


## Lives

# names from mockaroo
names = CSV.File("seed_data/names.csv") |> DataFrame!
lives = Dict()
for (i, row) in enumerate(eachrow(names))
    lives[i*2-1] = (
        name =  row.name_m .* row.last_name,
        birthday = rand(Date(1920,1,1):Day(1):Date(1999,12,31)),
        sex = :M,
        risk = rand([:Preferred,:Standard]),
        smoke = rand(Bool),
        )
    lives[i*2] = (
        name = row.name_f .* row.last_name,
        birthday = lives[i*2-1].birthday + Day(rand(-365*10:365*10)), # set the partner to be +/- 10 years old
        sex = :F,
        risk = rand([:Preferred,:Standard]),
        smoke = rand(Bool),
    )
        
        
        

end
n_lives = length(lives)


## Policies
policies = map(1:n_policies) do id
    joint = rand() < joint_pct
    id1 = rand(1:n_lives)
    id2 = iseven(id1) ? id1 - 1 : id1 + 1

    
    (
        id = id,
        joint = joint,
        
        life1_id = id1,
        life1_sex = lives[id1].sex,
        life1_risk = lives[id1].risk,
        life1_smoke = lives[id1].smoke,
        life1_birthday = lives[id1].birthday,

        life2_id = joint ? id2 : nothing,
        life2_sex = joint ? lives[id2].sex : nothing,
        life2_risk = joint ? lives[id2].risk : nothing,
        life2_smoke = joint ? lives[id2].smoke : nothing,
        life2_birthday = joint ? lives[id2].birthday : nothing,

        face = rand(100:50:5000) * 1e3,
        issue_date = lives[id1].birthday + Year(rand(25:60)) + Day(rand(1:365))

    )

end

CSV.write("output/policies.csv",policies |> DataFrame ,transform=(col, val) -> something(val, missing))

## Cessions
# By design of the sample data, is not fully captuing everything that we'd want to be ceded with the retention_limit above

reinsurers = [:ARe,:BRe,:CRe]
cessions = map(policies) do pol
    if pol.face > retention_limit && rand() > retention_slippage_rate
        reins = rand(reinsurers,rand(1:length(reinsurers)))
        return map(reins) do re_co 
            (
                pol_id = pol.id,
                ceded = (pol.face - retention_limit) / length(reins), 
                company=re_co,
                )
        end
    else
        return []
    end

end

CSV.write("output/cessions.csv",
    cessions |> Iterators.flatten |> collect |> DataFrame)