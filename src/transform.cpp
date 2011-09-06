/**************************************************************************************

Copyright © 2008-2010 VMware, Inc. All rights reserved.

Written by Mark Venguerov 2008 - 2010

**************************************************************************************/

#include "maps.h"
#include "queryprc.h"
#include "parser.h"
#include "expr.h"
#include "blob.h"

using namespace MVStoreKernel;

TransOp::~TransOp()
{
	if (fCopied && outs!=NULL) {
		for (ulong i=0; i<nOuts; i++) if (outs[i].vals!=NULL) {
			for (ulong j=0; j<outs[i].nValues; j++) freeV(outs[i].vals[j]);
			ses->free(outs[i].vals);
		}
		ses->free((void*)outs);
	}
}

void TransOp::connect(PINEx **results,unsigned nRes)
{
#if 0
	if (queryOp!=NULL && (nResults=queryOp->getNOuts())!=0) {
		if (nResults==1) queryOp->connect(&qr);
		else if ((results=new(ses) PINEx*[nResults])==NULL) return RC_NORESOURCES;
		else {
			memset(results,0,nResults*sizeof(PINEx*)); results[0]=&qr;
			for (unsigned i=1; i<nResults; i++) if ((results[i]=new(ses) PINEx(ses))==NULL) return RC_NORESOURCES;
			queryOp->connect(results,nResults);
		}
	}
#endif
}

RC TransOp::next(const PINEx *skip)
{
	RC rc=RC_OK; //unsigned nP=0; ulong i,np=nOuts; //min(npins,(unsigned)nOuts);
#if 0
//	for (i=0; i<np; ++nP,++i) if ((pins[i]=new(ses) PIN(ses,PIN::defPID,PageAddr::invAddr))==NULL) {rc=RC_NORESOURCES; break;}
	for (i=0; rc==RC_OK && i<np; i++) {
		PIN *pin=NULL/*pins[i]*/; const PIN *in; const ValueV &td=outs[i];
		for (ulong j=0; rc==RC_OK && j<td.nValues; j++) {
			const Value& v=td.vals[j],*pv=&v,*cv; Value w; ValueType ty=VT_ANY; ExprOp op=OP_SET;
			if (v.type==VT_VARREF && v.length==0) {
				if (v.type!=VT_VARREF || v.length!=0 || v.op!=OP_SET && v.op!=OP_ADD && v.op!=OP_ADD_BEFORE) rc=RC_INVPARAM;
	//			else if (v.refPath.refN>=nVars) {if ((v.meta&META_PROP_IFEXIST)==0) rc=RC_NOTFOUND;}
				else if ((in=vars[v.refPath.refN].pin)!=NULL) {
					const Value *props=in->properties; ulong nprops=in->nProperties;
					if (props!=NULL) for (ulong k=0; k<nprops; ++props,++k)
						if ((v.meta&META_PROP_IFNOTEXIST)==0 || pin->findProperty(props->property)==NULL) {
							w=*props; w.flags=NO_HEAP; w.op=v.op; w.meta|=META_PROP_DERIVED;
							if ((rc=pin->modify(&w,v.op==OP_ADD_BEFORE?STORE_FIRST_ELEMENT:STORE_COLLECTION_ID,w.eid,0,ses))!=RC_OK) break;
						}
				} else {
					if (vars[v.refPath.refN].pb.isNull() && (rc=ses->getStore()->queryMgr->getBody(vars[v.refPath.refN],TVO_READ))!=RC_OK)
						{if (rc==RC_NOTFOUND && (v.meta&META_PROP_IFEXIST)!=0) rc=RC_OK; continue;}
					const HeapPageMgr::HeapPIN *hpin=vars[v.refPath.refN].hpin;
					if (hpin!=NULL) {
// correct version ???
						const HeapPageMgr::HeapV *hprop=hpin->getPropTab();
						for (int k=hpin->nProps; --k>=0; hprop++) {
							PropertyID pid=hprop->getPropID();
							if ((v.meta&META_PROP_IFNOTEXIST)==0 || pin->findProperty(pid)==NULL) {
								if ((rc=ses->getStore()->queryMgr->loadVTx(w,hprop,vars[v.refPath.refN],LOAD_SSV,NULL))!=RC_OK) break;
								w.op=v.op; w.meta|=META_PROP_DERIVED;
								if ((rc=pin->modify(&w,v.op==OP_ADD_BEFORE?STORE_FIRST_ELEMENT:STORE_COLLECTION_ID,
														STORE_COLLECTION_ID,MODP_NCPY,ses))!=RC_OK) {freeV(w); break;}
							}
						}
					}
				}
				continue;
			}
			if ((v.meta&META_PROP_IFNOTEXIST)!=0) switch (v.property) {
			case PROP_SPEC_STAMP: continue;
			case PROP_SPEC_PINID: if (pin->id!=PIN::defPID) continue; else break;
			default: if (pin->findProperty(v.property)!=NULL) continue;
			}
			switch (v.type) {
			case VT_VARREF:
				if ((v.refPath.flags&RPTH_RESPIN)!=0) {
					if (v.refPath.refN>=nOuts && (v.meta&META_PROP_IFEXIST)==0) rc=RC_NOTFOUND;
					if (v.refPath.refN<np) {in=pins[v.refPath.refN]; assert(in!=NULL);} else continue;
					if (v.length==0) {w.set((IPIN*)in); pv=&w;}
					else if ((pv=in->findProperty(v.length==1?v.refPath.id:v.refPath.ids[0]))==NULL)
						{if ((v.meta&META_PROP_IFEXIST)==0) rc=RC_NOTFOUND; continue;}
				} else if (v.refPath.refN>=nVars) {
					if ((v.meta&META_PROP_IFEXIST)==0) rc=RC_NOTFOUND; continue;
				} else if ((in=vars[v.refPath.refN].pin)!=NULL) {
					if (v.length==0) {
						if (v.refPath.type!=VT_REF) {w.set(in->getPID()); pv=&w;}
						else {
							// copy pin
						}
					} else if ((pv=in->findProperty(v.length==1?v.refPath.id:v.refPath.ids[0]))==NULL)
						{if ((v.meta&META_PROP_IFEXIST)==0) rc=RC_NOTFOUND; continue;}
				} else {
					if (vars[v.refPath.refN].pb.isNull() && (rc=ses->getStore()->queryMgr->getPIN(vars[v.refPath.refN],ses))!=RC_OK)
						{if (rc==RC_NOTFOUND && (v.meta&META_PROP_IFEXIST)!=0) rc=RC_OK; continue;}
					ElementID eid=v.eid; pv=&w;
					if (eid!=STORE_FIRST_ELEMENT && eid!=STORE_LAST_ELEMENT) eid=STORE_COLLECTION_ID;
					rc=ses->getStore()->queryMgr->loadV(w,v.property,vars[v.refPath.refN],LOAD_SSV,NULL,eid);
					if (rc!=RC_OK) {if (rc==RC_NOTFOUND && (v.meta&META_PROP_IFEXIST)!=0) rc=RC_OK; continue;}
				}
				if (v.length>1) {
					//...
				}
				ty=(ValueType)v.refPath.type; break;
			case VT_PARAM:
				rc=RC_INTERNAL; continue;
			case VT_EXPR:
				if (v.te.fEval) {
					if ((rc=Expr::eval((Expr**)&v.expr,1,w,vars,nVars,NULL,0,ses))!=RC_OK)
						{if (rc==RC_NOTFOUND && (v.meta&META_PROP_IFEXIST)!=0) rc=RC_OK; continue;}
					ty=(ValueType)v.te.type; pv=&w;
				}
				break;
			default: break;
			}
			if (pv->type==VT_ARRAY || pv->type==VT_COLLECTION) switch (v.eid) {
			case STORE_COLLECTION_ID: break;
			case STORE_FIRST_ELEMENT:
			case STORE_LAST_ELEMENT:
				cv=pv->type==VT_ARRAY?&pv->varray[v.eid==STORE_FIRST_ELEMENT?0:pv->length-1]:
					pv->nav->navigate(v.eid==STORE_FIRST_ELEMENT?GO_FIRST:GO_LAST);
				if (cv==NULL) {if ((v.meta&META_PROP_IFEXIST)==0) rc=RC_NOTFOUND; continue;}
				if (pv!=&w) {if ((rc=copyV(*cv,w,ses))!=RC_OK) continue; pv=&w;}
				else {Value ww; if ((rc=copyV(*cv,ww,ses))!=RC_OK) continue; freeV(w); w=ww;}
				break;
			case STORE_SUM_COLLECTION: op=OP_PLUS; goto aggregate;
			case STORE_AVG_COLLECTION: op=OP_AVG; goto aggregate;
			case STORE_CONCAT_COLLECTION: op=OP_CONCAT; goto aggregate;
			case STORE_MIN_ELEMENT: op=OP_MIN; goto aggregate;
			case STORE_MAX_ELEMENT: op=OP_MAX;
			aggregate:
				if (pv!=&w) {w=*pv; w.flags=w.flags&~HEAP_TYPE_MASK|NO_HEAP; pv=&w;}
				if ((rc=Expr::calc(op,1,w,NULL,ses))!=RC_OK) continue;
				break;
			}
			if (ty!=VT_ANY && ty!=pv->type) {
				if (pv->type==VT_ARRAY) {
					for (ulong k=0; k<pv->length; k++) if (pv->varray[k].type!=ty) {
						if (pv!=&w) {
							//copy
						}
						if ((rc=convV(pv->varray[k],*(Value*)&pv->varray[k],ty))!=RC_OK) break;
					}
					if (rc!=RC_OK) continue;
				} else if (pv->type==VT_COLLECTION) {
					((Navigator*)pv->nav)->setType(ty);
				} else {
					if (pv!=&w) {w=*pv; w.flags=w.flags&~HEAP_TYPE_MASK|NO_HEAP; pv=&w;}
					if ((rc=convV(w,w,ty))!=RC_OK) continue;
				}
			}
			((Value*)pv)->op=v.op; 
			rc=pin->modify(pv,op==OP_ADD_BEFORE?STORE_FIRST_ELEMENT:STORE_LAST_ELEMENT,pv->eid,pv==&w?MODP_NCPY:0,ses);
		}
		if (rc==RC_OK && pins!=NULL) nOut++;
	}
	if (rc!=RC_OK) for (i=0; i<nP; i++) {pins[i]->destroy(); pins[i]=NULL;}
#endif
	return rc;
}

void TransOp::getOpDescr(QODescr& qdscr)
{
	queryOp->getOpDescr(qdscr); qdscr.level++;
	//???
}

void TransOp::print(SOutCtx& buf,int level) const
{
	buf.fill('\t',level); buf.append("access: ",8);
	for (unsigned i=0; i<nOuts; i++) {
	}
	buf.append("\n",1);
}
