/**************************************************************************************

Copyright © 2004-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2004 - 2010

**************************************************************************************/

#include "session.h"
#include "mvstoreimpl.h"

using namespace	MVStore;
using namespace MVStoreKernel;

const static struct UnitDscr
{
	const char		*shortName;
	const char		*longName;
	uint32_t		dims;
	double			factor;
	double			shift;
} unitDscrs[] = 
{
	{NULL,					NULL,					0x37777777,		0.,					0.},
	{"m",					"meter",				0x37777778,		1.,					0.},
	{"kg",					"kilogram",				0x37777787,		1.,					0.},
	{"s",					"second",				0x37777877,		1.,					0.},
	{"A",					"ampere",				0x37778777,		1.,					0.},
	{"K",					"kelvin",				0x37787777,		1.,					0.},
	{"mol",					"mole",					0x37877777,		1.,					0.},
	{"cd",					"candela",				0x38777777,		1.,					0.},

	{"Hz",					"hertz",				0x37777677,		1.,					0.},
	{"N",					"newton",				0x37777588,		1.,					0.},
	{"Pa",					"pascal",				0x37777586,		1.,					0.},
	{"J",					"joule",				0x37777589,		1.,					0.},
	{"W",					"watt",					0x37777489,		1.,					0.},
	{"C",					"coulomb",				0x37778877,		1.,					0.},
	{"V",					"volt",					0x37776489,		1.,					0.},
	{"F",					"farad",				0x37779B65,		1.,					0.},
	{"Ohm",					"ohm",					0x37775489,		1.,					0.},
	{"S",					"siemens",				0x37779A65,		1.,					0.},
	{"Wb",					"weber",				0x37776589,		1.,					0.},
	{"T",					"tesla",				0x37776587,		1.,					0.},
	{"H",					"henri",				0x37775589,		1.,					0.},
	{"dC",					"degree Celsius",		0x37787777,		1.,					273.15},
	{"rad",					"radian",				0x37777777,		1.,					0.},
	{"sr",					"steradian",			0x47777777,		1.,					0.},
	{"lm",					"lumen",				0x48777777,		1.,					0.},
	{"lx",					"lux",					0x28777775,		1.,					0.},
	{"Bq",					"becquerel",			0x37777677,		1.,					0.},
	{"Gy",					"gray",					0x37777579,		1.,					0.},
	{"Sv",					"sievert",				0x37777579,		1.,					0.},
	{"kat",					"katal",				0x37877677,		1.,					0.},

	{"dm",					"decimeter",			0x37777778,		1.E-1,				0.},
	{"cm",					"centimeter",			0x37777778,		1.E-2,				0.},
	{"mm",					"millimeter",			0x37777778,		1.E-3,				0.},
	{"mkm",					"micrometer",			0x37777778,		1.E-6,				0.},
	{"nm",					"nanometer",			0x37777778,		1.E-9,				0.},
	{"km",					"kilometer",			0x37777778,		1.E+3,				0.},
	{"in",					"inch",					0x37777778,		0.0254,				0.},
	{"ft",					"foot",					0x37777778,		0.3048,				0.},
	{"yd",					"yard",					0x37777778,		0.9144,				0.},
	{"mi",					"mile",					0x37777778,		1609.344,			0.},
	{"nmi",					"nautical mile",		0x37777778,		1852.,				0.},

	{"au",					"astronomical unit",	0x37777778,		149597870691.,		0.},
	{"pc",					"parsec",				0x37777778,		30.85678E+15,		0.},
	{"ly",					"light year",			0x37777778,		9.460730473E+15,	0.},

	{"mps",					"meters per second",	0x37777678,		1.,					0.},
	{"kph",					"kilometers per hour",	0x37777678,		0.277777777777777777778,	0.},
	{"fpm",					"feet per minute",		0x37777678,		0.00508,			0.},
	{"mph",					"miles per hour",		0x37777678,		0.44704,			0.},
	{"kt",					"knot",					0x37777678,		0.514444444444444444444,	0.},

	{"g",					"gram",					0x37777787,		1.E-3,				0.},
	{"mg",					"milligram",			0x37777787,		1.E-6,				0.},
	{"mkg",					"microgram",			0x37777787,		1.E-9,				0.},
	{"t",					"ton",					0x37777787,		1000.,				0.},

	{"lb",					"pound",				0x37777787,		0.45359237,			0.},
	{"oz",					"ounce",				0x37777787,		0.0283495231,		0.},
	{"st",					"stone",				0x37777787,		6.35029,			0.},
	 
	{"m2",					"square meter",			0x37777779,		1.,					0.},
	{"cm2",					"square centimeter",	0x37777779,		1.E-4,				0.},
	{"sqin",				"square inch",			0x37777779,		6.4516E-4,			0.},
	{"sqft",				"square foot",			0x37777779,		0.09290304,			0.},
	{"ac",					"acre",					0x37777779,		4046.873,			0.},
	{"ha",					"hectare",				0x37777779,		10000.,				0.},

	{"m3",					"cubic meter",			0x3777777A,		1.,					0.},
	{"l",					"liter",				0x3777777A,		1.E-3,				0.},
	{"cl",					"centiliter",			0x3777777A,		1.E-5,				0.},
	{"cm3",					"cubic centimeter",		0x3777777A,		1.E-6,				0.},
	{"cf",					"cubic foot",			0x3777777A,		0.02831685,			0.},
	{"ci",					"cubic inch",			0x3777777A,		1.63871E-5,			0.},
	{"floz",				"fluid once",			0x3777777A,		29.573531E-6,		0.},	// american

	{"bbl",					"barrel",				0x3777777A,		0.158987,			0.},	// petroleum
	{"bu",					"bushel",				0x3777777A,		3.523907E-2,		0.},	// american
	{"gal",					"gallon",				0x3777777A,		3.785411784E-3,		0.},	// american
	{"qt",					"quart",				0x3777777A,		0.946352946E-3,		0.},	// american
	{"pt",					"pint",					0x3777777A,		0.473176473E-3,		0.},	// american

	{"b",					"bar",					0x37777586,		1.E+5,				0.},
	{"mmHg",				"millimeters of mercury",0x37777586,	133.3,				0.},
	{"inHg",				"inches of mercury",	0x37777586,		3.38638E+3,			0.},

	{"cal",					"calorie",				0x37777589,		4.1868,				0.}, 
	{"kcal",				"Calorie",				0x37777589,		4.1868E+3,			0.}, 
	{"ct",					"carat",				0x37777787,		2.E-4,				0.}, 
	{"dF",					"degrees Fahrenheit",	0x37787777,		0.5555555555556,	255.3722222}, 
};

bool MVStoreKernel::compatible(QualifiedValue& q1, QualifiedValue& q2)
{
	if (q1.units==q2.units || q1.units==Un_NDIM || q2.units==Un_NDIM) return true;
	if (q1.units>=Un_ALL || q2.units>=Un_ALL || unitDscrs[q1.units].dims!=unitDscrs[q2.units].dims) return false;
	const UnitDscr& pd1=unitDscrs[q1.units],&pd2=unitDscrs[q2.units];
	if (pd1.factor==1. && pd1.shift==0.) {q2.d=q2.d*pd2.factor+pd2.shift; q2.units=q1.units;}
	else if (pd2.factor==1.) {q1.d=q1.d*pd1.factor+pd1.shift-pd2.shift; q1.units=q2.units;}
	else {q2.d=(q2.d*pd2.factor+pd2.shift-pd1.shift)/pd1.factor; q2.units=q1.units;}
	return true;
}

bool MVStoreKernel::compatibleMulDiv(Value& v, uint16_t units,bool fDiv)
{
	if (!fDiv && v.qval.units==Un_NDIM) {v.qval.units=units; return true;}
	if (v.qval.units<Un_ALL && units<Un_ALL && (v.type==VT_FLOAT||v.type==VT_DOUBLE)) {
		const UnitDscr& pd1=unitDscrs[v.qval.units],&pd2=unitDscrs[units];
		if (fDiv && pd1.shift!=0.) return false; uint16_t units=uint16_t(~0u);
		uint32_t newDim=fDiv?pd1.dims-pd2.dims+0x37777777:pd1.dims+pd2.dims-0x37777777;
		if (newDim==0x37777777) units=pd1.dims==0x37777779?Un_sr:Un_NDIM;
		else for (unsigned i=1; i<Un_ALL; i++)
			if (unitDscrs[i].dims==newDim) {units=i; if (unitDscrs[i].factor==1. && unitDscrs[i].shift==0.) break;}
		if (units!=uint16_t(~0u)) {
			if (pd1.factor*pd2.factor!=1.0||pd1.shift!=0.||pd2.shift!=0.||unitDscrs[units].factor!=1.0||unitDscrs[units].shift!=0) {
				double d=v.type==VT_FLOAT?double(v.f):v.d;
				d=d*pd1.factor+pd1.shift; if (fDiv) d/=pd2.factor; else d=d*pd2.factor+pd2.shift;
				d=(d-unitDscrs[units].shift)/unitDscrs[units].factor; if (v.type==VT_FLOAT) v.f=(float)d; else v.d=d;
			}
			v.qval.units=units; return true;
		}
	}
	return false;
}

Units MVStoreKernel::getUnits(const char *suffix,size_t l)
{
	if (suffix!=NULL && l!=0) for (unsigned i=1; i<sizeof(unitDscrs)/sizeof(unitDscrs[0]); i++)
		if (!strncmp(unitDscrs[i].shortName,suffix,l) && unitDscrs[i].shortName[l]=='\0') return (Units)i;
	return Un_NDIM;
}
