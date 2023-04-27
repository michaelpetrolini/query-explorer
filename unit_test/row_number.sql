select
   ROW_NUMBER() OVER(PARTITION BY a) rn1,
   ROW_NUMBER() OVER(PARTITION BY a, b) rn2,
   ROW_NUMBER() OVER(PARTITION BY a, b ORDER BY c) rn3,
   ROW_NUMBER() OVER(PARTITION BY a, b ORDER BY c, d) rn4,
   ROW_NUMBER() OVER(PARTITION BY a, b ORDER BY c desc, d) rn5,
   ROW_NUMBER() OVER (PARTITION BY a, b ORDER BY c desc, d) rn6
  from `alpha_project.characters.letters`