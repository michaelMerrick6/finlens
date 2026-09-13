// Original decorative SVG; no external assets or runtime animation.
export function CapitolIllustration() {
  return <svg className="capitol-illustration" viewBox="-70 0 760 430" fill="none" aria-hidden="true" focusable="false">
    <defs>
      <linearGradient id="capitol-fade" x1="0" y1="0" x2="0" y2="1"><stop offset="0.68" stopColor="white"/><stop offset="1" stopColor="black"/></linearGradient>
      <mask id="capitol-mask"><rect x="-70" width="760" height="430" fill="url(#capitol-fade)"/></mask>
    </defs>
    <g mask="url(#capitol-mask)" stroke="currentColor" strokeWidth="1.1" strokeLinejoin="round" strokeLinecap="round">
      <g fill="#fafbf8">
        <path d="M-35 305h200v-17h70v-26h150v26h70v17h200v91H-35z"/>
        <path d="M230 263v-55h160v55M224 208h172v8H224zM230 199h160v9H230z"/>
        <path d="M243 198c4-44 24-78 48-95h38c24 17 44 51 48 95z"/>
        <path d="M286 104v-9h48v9M291 95V70q19-13 38 0v25M286 69q24-18 48 0z"/>
        <path d="M300 57v-8h20v8M304 49V34h12v15M310 34V18m-4 12 4-12 5 13m-8-10 3-8 4 9"/>
        <path d="M222 266h176v9H222zM237 284h146v96H237zM225 284l85-25 85 25z"/>
      </g>
      {[252,263,277,293,310,327,343,357,368].map((x,i)=><path key={x} d={`M${x} 197 Q${x+(310-x)*.22} 142 ${300+i*2.5} 104`} opacity=".8"/>)}
      <path d="M244 178q66-13 132 0M239 193q71-12 142 0M239 185q71-12 142 0"/>
      {[297,306,315,324].map(x=><path key={x} d={`M${x} 91V75q3-5 5 0v16`}/>)}
      {Array.from({length:12},(_,i)=>240+i*12).map(x=><g key={x}><path d={`M${x} 253v-32h5v32m-7 3h9`}/><path d={`M${x+8} 247v-23`} opacity=".4"/></g>)}
      {Array.from({length:7},(_,i)=>252+i*18).map(x=><g key={x}><path d={`M${x} 365v-73h7v73m-9 3h11`}/><path d={`M${x+3} 296v64`} opacity=".35"/></g>)}
      <path d="M224 375h173v6H224zM212 382h198v6H212zM197 389h228v6H197zM182 396h258v6H182zM165 403h292"/>
      {[-15,17,55,87,119,174,204,409,439,490,522,554,592,624].map(x=><g key={x}><path d={`M${x} 335v-18q7-8 14 0v18zM${x} 371v-19q7-8 14 0v19z`}/><path d={`M${x+5} 320v12m0 22v14`} opacity=".4"/></g>)}
      <path d="M-35 342h269m151 0h270M-35 380h269m151 0h270M-45 301h210m290 0h210M164 285h70m152 0h70M-50 395h235m255 0h225"/>
      <path d="M178 284v-15q14-17 28 0v15m208 0v-15q14-17 28 0v15M182 269h20m216 0h20"/>
      <path d="M274 282h72M285 278h50M297 274h26" opacity=".45"/>
      <path d="M-30 414h680M-5 423h630" opacity=".4"/>
    </g>
  </svg>;
}
