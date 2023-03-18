SELECT
	alpha.a as letter_a,
	alpha.b as letter_b,
	alpha.c as letter_c
FROM
	letters.alphabet alpha
WHERE alpha.d = "e"