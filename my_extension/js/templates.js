this["JST"] = this["JST"] || {};

this["JST"]["src/browser_action/templates/departments.hbs"] = Handlebars.template({"1":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return ((stack1 = (lookupProperty(helpers,"ifGreaterThanZero")||(depth0 && lookupProperty(depth0,"ifGreaterThanZero"))||container.hooks.helperMissing).call(depth0 != null ? depth0 : (container.nullContext || {}),(depth0 != null ? lookupProperty(depth0,"1") : depth0),{"name":"ifGreaterThanZero","hash":{},"fn":container.program(2, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":2,"column":2},"end":{"line":6,"column":24}}})) != null ? stack1 : "");
},"2":function(container,depth0,helpers,partials,data) {
    var stack1, helper, options, alias1=container.lambda, alias2=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    }, buffer = 
  "    <div class=\"department-filter btn-white btn-sm\" data-department=\""
    + alias2(alias1((depth0 != null ? lookupProperty(depth0,"0") : depth0), depth0))
    + "\">\n      ";
  stack1 = ((helper = (helper = lookupProperty(helpers,"departmentName") || (depth0 != null ? lookupProperty(depth0,"departmentName") : depth0)) != null ? helper : container.hooks.helperMissing),(options={"name":"departmentName","hash":{},"fn":container.program(3, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":4,"column":6},"end":{"line":4,"column":56}}}),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),options) : helper));
  if (!lookupProperty(helpers,"departmentName")) { stack1 = container.hooks.blockHelperMissing.call(depth0,stack1,options)}
  if (stack1 != null) { buffer += stack1; }
  return buffer + ": "
    + alias2(alias1((depth0 != null ? lookupProperty(depth0,"1") : depth0), depth0))
    + "\n    </div>\n";
},"3":function(container,depth0,helpers,partials,data) {
    var lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return container.escapeExpression(container.lambda((depth0 != null ? lookupProperty(depth0,"0") : depth0), depth0));
},"5":function(container,depth0,helpers,partials,data) {
    return "  <div class=\"more-departments-button btn-white btn-sm\">●●●</div>\n";
},"compiler":[8,">= 4.3.0"],"main":function(container,depth0,helpers,partials,data) {
    var stack1, alias1=depth0 != null ? depth0 : (container.nullContext || {}), lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return ((stack1 = lookupProperty(helpers,"each").call(alias1,(depth0 != null ? lookupProperty(depth0,"departments") : depth0),{"name":"each","hash":{},"fn":container.program(1, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":1,"column":0},"end":{"line":7,"column":9}}})) != null ? stack1 : "")
    + ((stack1 = (lookupProperty(helpers,"ifGreaterThanZero")||(depth0 && lookupProperty(depth0,"ifGreaterThanZero"))||container.hooks.helperMissing).call(alias1,((stack1 = ((stack1 = (depth0 != null ? lookupProperty(depth0,"departments") : depth0)) != null ? lookupProperty(stack1,"3") : stack1)) != null ? lookupProperty(stack1,"1") : stack1),{"name":"ifGreaterThanZero","hash":{},"fn":container.program(5, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":8,"column":0},"end":{"line":10,"column":22}}})) != null ? stack1 : "");
},"useData":true});

this["JST"]["src/browser_action/templates/finder.hbs"] = Handlebars.template({"1":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return container.escapeExpression(((helper = (helper = lookupProperty(helpers,"email") || (depth0 != null ? lookupProperty(depth0,"email") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"email","hash":{},"data":data,"loc":{"start":{"line":5,"column":99},"end":{"line":5,"column":108}}}) : helper)));
},"3":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "          <span class=\"tag tag--success\" data-toggle=\"tooltip\" data-placement=\"top\" title=\"Valid\" data-locale-title=\"valid\">\n            <span class=\"tag__label\">\n              <i aria-hidden=\"true\" class=\"tag__icon fas fa-shield-check\"></i>\n              "
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"score") || (depth0 != null ? lookupProperty(depth0,"score") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"score","hash":{},"data":data,"loc":{"start":{"line":16,"column":14},"end":{"line":16,"column":23}}}) : helper)))
    + "%\n            </span>\n          </span>\n";
},"5":function(container,depth0,helpers,partials,data) {
    var stack1, helper, options, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    }, buffer = "";

  stack1 = ((helper = (helper = lookupProperty(helpers,"ifIsAcceptAll") || (depth0 != null ? lookupProperty(depth0,"ifIsAcceptAll") : depth0)) != null ? helper : container.hooks.helperMissing),(options={"name":"ifIsAcceptAll","hash":{},"fn":container.program(6, data, 0),"inverse":container.program(8, data, 0),"data":data,"loc":{"start":{"line":20,"column":10},"end":{"line":36,"column":28}}}),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),options) : helper));
  if (!lookupProperty(helpers,"ifIsAcceptAll")) { stack1 = container.hooks.blockHelperMissing.call(depth0,stack1,options)}
  if (stack1 != null) { buffer += stack1; }
  return buffer;
},"6":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "            <span class=\"tag tag--warning\" data-toggle=\"tooltip\" data-placement=\"top\" title=\"Accept All\" data-locale-title=\"accept_all\">\n              <span class=\"tag__label\">\n                <i aria-hidden=\"true\" class=\"tag__icon fas fa-shield-check\"></i>\n                "
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"score") || (depth0 != null ? lookupProperty(depth0,"score") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"score","hash":{},"data":data,"loc":{"start":{"line":24,"column":16},"end":{"line":24,"column":25}}}) : helper)))
    + "%\n              </span>\n            </span>\n";
},"8":function(container,depth0,helpers,partials,data) {
    var helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "            <div class=\"verification-result\">\n              <span class=\"tag tag--success\" data-toggle=\"tooltip\" data-placement=\"top\" title=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"score") || (depth0 != null ? lookupProperty(depth0,"score") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"score","hash":{},"data":data,"loc":{"start":{"line":29,"column":95},"end":{"line":29,"column":104}}}) : helper)))
    + "%\">\n                <span class=\"score mr-1 "
    + alias4(((helper = (helper = lookupProperty(helpers,"confidence_score_class") || (depth0 != null ? lookupProperty(depth0,"confidence_score_class") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"confidence_score_class","hash":{},"data":data,"loc":{"start":{"line":30,"column":40},"end":{"line":30,"column":66}}}) : helper)))
    + "\"></span>\n                <span class=\"tag__label\">\n                  "
    + alias4(((helper = (helper = lookupProperty(helpers,"score") || (depth0 != null ? lookupProperty(depth0,"score") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"score","hash":{},"data":data,"loc":{"start":{"line":32,"column":18},"end":{"line":32,"column":27}}}) : helper)))
    + "%\n                </span>\n              </span>\n            </div>\n";
},"10":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "      <div class=\"ds-result__attribute\">\n        <span class=\"far fa-phone\"></span>\n        "
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"phone_number") || (depth0 != null ? lookupProperty(depth0,"phone_number") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"phone_number","hash":{},"data":data,"loc":{"start":{"line":44,"column":8},"end":{"line":44,"column":24}}}) : helper)))
    + "\n      </div>\n";
},"12":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "      <div class=\"ds-result__attribute\">\n        <span class=\"far fa-briefcase\"></span>\n        "
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"position") || (depth0 != null ? lookupProperty(depth0,"position") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"position","hash":{},"data":data,"loc":{"start":{"line":50,"column":8},"end":{"line":50,"column":20}}}) : helper)))
    + "\n      </div>\n";
},"14":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return ((stack1 = lookupProperty(helpers,"if").call(depth0 != null ? depth0 : (container.nullContext || {}),(depth0 != null ? lookupProperty(depth0,"department") : depth0),{"name":"if","hash":{},"fn":container.program(15, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":52,"column":6},"end":{"line":58,"column":6}}})) != null ? stack1 : "");
},"15":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "      <div class=\"ds-result__attribute\">\n        <span class=\"tag tag--sm ds-result__department\">\n          <span class=\"tag__label\">"
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"department") || (depth0 != null ? lookupProperty(depth0,"department") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"department","hash":{},"data":data,"loc":{"start":{"line":55,"column":35},"end":{"line":55,"column":49}}}) : helper)))
    + "</span>\n        </span>\n      </div>\n      ";
},"17":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "        <a class=\"ds-result__social\" href=\""
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"linkedin") || (depth0 != null ? lookupProperty(depth0,"linkedin") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"linkedin","hash":{},"data":data,"loc":{"start":{"line":61,"column":43},"end":{"line":61,"column":55}}}) : helper)))
    + "\" rel=\"noopener noreferrer nofollow\" target=\"_blank\" aria-label=\"LinkedIn Profile\">\n          <span class=\"fab fa-linkedin\" aria-hidden=\"true\"></span>\n        </a>\n";
},"19":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "        <a class=\"ds-result__social\" href=\"https://www.twitter.com/"
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"twitter") || (depth0 != null ? lookupProperty(depth0,"twitter") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"twitter","hash":{},"data":data,"loc":{"start":{"line":66,"column":67},"end":{"line":66,"column":78}}}) : helper)))
    + "\" rel=\"noopener noreferrer nofollow\" target=\"_blank\" aria-label=\"Twitter Profile\">\n          <span class=\"fab fa-twitter\" aria-hidden=\"true\"></span>\n        </a>\n";
},"21":function(container,depth0,helpers,partials,data,blockParams,depths) {
    var stack1, helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "  <div class=\"ds-result__sources\">\n    <div class=\"ds-sources__count\">"
    + ((stack1 = ((helper = (helper = lookupProperty(helpers,"method") || (depth0 != null ? lookupProperty(depth0,"method") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(alias1,{"name":"method","hash":{},"data":data,"loc":{"start":{"line":79,"column":35},"end":{"line":79,"column":47}}}) : helper))) != null ? stack1 : "")
    + "</div>\n    <ul class=\"ds-sources-list\">\n"
    + ((stack1 = lookupProperty(helpers,"each").call(alias1,(depth0 != null ? lookupProperty(depth0,"sources") : depth0),{"name":"each","hash":{},"fn":container.program(22, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":81,"column":6},"end":{"line":95,"column":15}}})) != null ? stack1 : "")
    + "    </ul>\n  </div>\n";
},"22":function(container,depth0,helpers,partials,data,blockParams,depths) {
    var stack1, helper, options, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.lambda, alias3=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    }, buffer = 
  "      <li class=\"ds-sources-list__item "
    + ((stack1 = lookupProperty(helpers,"unless").call(alias1,(depth0 != null ? lookupProperty(depth0,"still_on_page") : depth0),{"name":"unless","hash":{},"fn":container.program(23, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":82,"column":39},"end":{"line":82,"column":111}}})) != null ? stack1 : "")
    + "\">\n"
    + ((stack1 = lookupProperty(helpers,"unless").call(alias1,(depth0 != null ? lookupProperty(depth0,"still_on_page") : depth0),{"name":"unless","hash":{},"fn":container.program(25, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":83,"column":8},"end":{"line":89,"column":19}}})) != null ? stack1 : "")
    + "        <a class=\"ds-sources-list__link\" href=\""
    + alias3(alias2((depth0 != null ? lookupProperty(depth0,"uri") : depth0), depth0))
    + "#hunter-email:"
    + alias3(alias2((depths[1] != null ? lookupProperty(depths[1],"value") : depths[1]), depth0))
    + "\" rel=\"noopener noreferrer nofollow\" target=\"_blank\">\n          "
    + alias3(alias2((depth0 != null ? lookupProperty(depth0,"uri") : depth0), depth0))
    + "\n        </a>\n        <time class=\"ds-sources-list__date\">";
  stack1 = ((helper = (helper = lookupProperty(helpers,"userDate") || (depth0 != null ? lookupProperty(depth0,"userDate") : depth0)) != null ? helper : container.hooks.helperMissing),(options={"name":"userDate","hash":{},"fn":container.program(27, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":93,"column":44},"end":{"line":93,"column":91}}}),(typeof helper === "function" ? helper.call(alias1,options) : helper));
  if (!lookupProperty(helpers,"userDate")) { stack1 = container.hooks.blockHelperMissing.call(depth0,stack1,options)}
  if (stack1 != null) { buffer += stack1; }
  return buffer + "</time>\n      </li>\n";
},"23":function(container,depth0,helpers,partials,data) {
    return "ds-sources-list__item--outdated";
},"25":function(container,depth0,helpers,partials,data) {
    return "        <span class=\"tag tag--sm\">\n          <span class=\"tag__label\">\n          Removed\n          </span>\n        </span>\n";
},"27":function(container,depth0,helpers,partials,data) {
    var lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return container.escapeExpression(container.lambda((depth0 != null ? lookupProperty(depth0,"extracted_on") : depth0), depth0));
},"compiler":[8,">= 4.3.0"],"main":function(container,depth0,helpers,partials,data,blockParams,depths) {
    var stack1, helper, options, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    }, buffer = 
  "<div class=\"ds-result ds-result--single\">\n  <div class=\"ds-result__data\">\n    <div class=\"ds-result__avatar\">\n      <figure class=\"h-avatar h-avatar--auto\">\n        <img alt=\"\" onload=\"this.style='opacity: 1;'\" src=\"https://www.gravatar.com/avatar/";
  stack1 = ((helper = (helper = lookupProperty(helpers,"md5") || (depth0 != null ? lookupProperty(depth0,"md5") : depth0)) != null ? helper : alias2),(options={"name":"md5","hash":{},"fn":container.program(1, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":5,"column":91},"end":{"line":5,"column":116}}}),(typeof helper === alias3 ? helper.call(alias1,options) : helper));
  if (!lookupProperty(helpers,"md5")) { stack1 = container.hooks.blockHelperMissing.call(depth0,stack1,options)}
  if (stack1 != null) { buffer += stack1; }
  return buffer + "?s=128&d=https%3A%2F%2Fui-avatars.com/api/"
    + alias4(((helper = (helper = lookupProperty(helpers,"first_name") || (depth0 != null ? lookupProperty(depth0,"first_name") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"first_name","hash":{},"data":data,"loc":{"start":{"line":5,"column":158},"end":{"line":5,"column":172}}}) : helper)))
    + "%2B"
    + alias4(((helper = (helper = lookupProperty(helpers,"last_name") || (depth0 != null ? lookupProperty(depth0,"last_name") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"last_name","hash":{},"data":data,"loc":{"start":{"line":5,"column":175},"end":{"line":5,"column":188}}}) : helper)))
    + "/128/E6F4FF/0578D6\">\n      </figure>\n    </div>\n    <div class=\"ds-result__primary\">\n      <div class=\"ds-result__fullname\">"
    + alias4(((helper = (helper = lookupProperty(helpers,"first_name") || (depth0 != null ? lookupProperty(depth0,"first_name") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"first_name","hash":{},"data":data,"loc":{"start":{"line":9,"column":39},"end":{"line":9,"column":53}}}) : helper)))
    + " "
    + alias4(((helper = (helper = lookupProperty(helpers,"last_name") || (depth0 != null ? lookupProperty(depth0,"last_name") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"last_name","hash":{},"data":data,"loc":{"start":{"line":9,"column":54},"end":{"line":9,"column":67}}}) : helper)))
    + "</div>\n      <div class=\"ds-result__email copy-email\" data-email=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"email") || (depth0 != null ? lookupProperty(depth0,"email") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"email","hash":{},"data":data,"loc":{"start":{"line":10,"column":59},"end":{"line":10,"column":68}}}) : helper)))
    + "\" data-toggle=\"tooltip\" data-placement=\"top\" title=\"Click to copy\" data-locale-title=\"click_to_copy\">"
    + alias4(((helper = (helper = lookupProperty(helpers,"email") || (depth0 != null ? lookupProperty(depth0,"email") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"email","hash":{},"data":data,"loc":{"start":{"line":10,"column":169},"end":{"line":10,"column":178}}}) : helper)))
    + "</div>\n      <div class=\"ds-result__verification\">\n"
    + ((stack1 = (lookupProperty(helpers,"ifIsVerified")||(depth0 && lookupProperty(depth0,"ifIsVerified"))||alias2).call(alias1,((stack1 = (depth0 != null ? lookupProperty(depth0,"verification") : depth0)) != null ? lookupProperty(stack1,"status") : stack1),{"name":"ifIsVerified","hash":{},"fn":container.program(3, data, 0, blockParams, depths),"inverse":container.program(5, data, 0, blockParams, depths),"data":data,"loc":{"start":{"line":12,"column":8},"end":{"line":37,"column":25}}})) != null ? stack1 : "")
    + "      </div>\n    </div>\n    <div class=\"ds-result__secondary\">\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"phone_number") : depth0),{"name":"if","hash":{},"fn":container.program(10, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":41,"column":6},"end":{"line":46,"column":13}}})) != null ? stack1 : "")
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"position") : depth0),{"name":"if","hash":{},"fn":container.program(12, data, 0, blockParams, depths),"inverse":container.program(14, data, 0, blockParams, depths),"data":data,"loc":{"start":{"line":47,"column":6},"end":{"line":58,"column":13}}})) != null ? stack1 : "")
    + "      <div class=\"ds-result__attribute\">\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"linkedin") : depth0),{"name":"if","hash":{},"fn":container.program(17, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":60,"column":8},"end":{"line":64,"column":15}}})) != null ? stack1 : "")
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"twitter") : depth0),{"name":"if","hash":{},"fn":container.program(19, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":65,"column":8},"end":{"line":69,"column":15}}})) != null ? stack1 : "")
    + "      </div>\n    </div>\n    <div class=\"ds-result__save\">\n      <button class=\"h-button h-button--sm save-lead-button\" type=\"button\" data-email=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"email") || (depth0 != null ? lookupProperty(depth0,"email") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"email","hash":{},"data":data,"loc":{"start":{"line":73,"column":87},"end":{"line":73,"column":96}}}) : helper)))
    + "\" data-locale=\"save_as_lead\">Save as lead</button>\n    </div>\n  </div>\n\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"sources") : depth0),{"name":"if","hash":{},"fn":container.program(21, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":77,"column":2},"end":{"line":98,"column":9}}})) != null ? stack1 : "")
    + "</div>\n";
},"useData":true,"useDepths":true});

this["JST"]["src/browser_action/templates/search_results.hbs"] = Handlebars.template({"1":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "      <div class=\"ds-result__fullname\">"
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"full_name") || (depth0 != null ? lookupProperty(depth0,"full_name") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"full_name","hash":{},"data":data,"loc":{"start":{"line":5,"column":39},"end":{"line":5,"column":52}}}) : helper)))
    + "</div>\n";
},"3":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "          <span class=\"tag tag--success\" data-toggle=\"tooltip\" data-placement=\"top\" title=\"Valid\" data-locale-title=\"valid\">\n            <span class=\"tag__label\">\n              <i aria-hidden=\"true\" class=\"tag__icon fas fa-shield-check\"></i>\n              "
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"confidence") || (depth0 != null ? lookupProperty(depth0,"confidence") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"confidence","hash":{},"data":data,"loc":{"start":{"line":13,"column":14},"end":{"line":13,"column":28}}}) : helper)))
    + "%\n            </span>\n          </span>\n";
},"5":function(container,depth0,helpers,partials,data) {
    var stack1, helper, options, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    }, buffer = "";

  stack1 = ((helper = (helper = lookupProperty(helpers,"ifIsAcceptAll") || (depth0 != null ? lookupProperty(depth0,"ifIsAcceptAll") : depth0)) != null ? helper : container.hooks.helperMissing),(options={"name":"ifIsAcceptAll","hash":{},"fn":container.program(6, data, 0),"inverse":container.program(8, data, 0),"data":data,"loc":{"start":{"line":17,"column":10},"end":{"line":36,"column":28}}}),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),options) : helper));
  if (!lookupProperty(helpers,"ifIsAcceptAll")) { stack1 = container.hooks.blockHelperMissing.call(depth0,stack1,options)}
  if (stack1 != null) { buffer += stack1; }
  return buffer;
},"6":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "            <span class=\"tag tag--warning\" data-toggle=\"tooltip\" data-placement=\"top\" title=\"Accept All\" data-locale-title=\"accept_all\">\n              <span class=\"tag__label\">\n                <i aria-hidden=\"true\" class=\"tag__icon fas fa-shield-check\"></i>\n                "
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"confidence") || (depth0 != null ? lookupProperty(depth0,"confidence") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"confidence","hash":{},"data":data,"loc":{"start":{"line":21,"column":16},"end":{"line":21,"column":30}}}) : helper)))
    + "%\n              </span>\n            </span>\n";
},"8":function(container,depth0,helpers,partials,data) {
    var helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "            <div class=\"verification-result\">\n              <span class=\"tag tag--success\" data-toggle=\"tooltip\" data-placement=\"top\" title=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"confidence") || (depth0 != null ? lookupProperty(depth0,"confidence") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"confidence","hash":{},"data":data,"loc":{"start":{"line":26,"column":95},"end":{"line":26,"column":109}}}) : helper)))
    + "%\">\n                <span class=\"score mr-1 "
    + alias4(((helper = (helper = lookupProperty(helpers,"confidence_score_class") || (depth0 != null ? lookupProperty(depth0,"confidence_score_class") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"confidence_score_class","hash":{},"data":data,"loc":{"start":{"line":27,"column":40},"end":{"line":27,"column":66}}}) : helper)))
    + "\"></span>\n                <span class=\"tag__label\">\n                  "
    + alias4(((helper = (helper = lookupProperty(helpers,"confidence") || (depth0 != null ? lookupProperty(depth0,"confidence") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"confidence","hash":{},"data":data,"loc":{"start":{"line":29,"column":18},"end":{"line":29,"column":32}}}) : helper)))
    + "%\n                </span>\n              </span>\n            </div>\n            <button type=\"button\" class=\"h-button h-button--xs verification-link\" data-email=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"value") || (depth0 != null ? lookupProperty(depth0,"value") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"value","hash":{},"data":data,"loc":{"start":{"line":33,"column":94},"end":{"line":33,"column":103}}}) : helper)))
    + "\" data-locale=\"verify_email\">\n              Verify email\n            </button>\n";
},"10":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "      <div class=\"ds-result__attribute\">\n        <span class=\"far fa-phone\"></span>\n        "
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"phone_number") || (depth0 != null ? lookupProperty(depth0,"phone_number") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"phone_number","hash":{},"data":data,"loc":{"start":{"line":44,"column":8},"end":{"line":44,"column":24}}}) : helper)))
    + "\n      </div>\n";
},"12":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "      <div class=\"ds-result__attribute\">\n        <span class=\"far fa-briefcase\"></span>\n        "
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"position") || (depth0 != null ? lookupProperty(depth0,"position") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"position","hash":{},"data":data,"loc":{"start":{"line":50,"column":8},"end":{"line":50,"column":20}}}) : helper)))
    + "\n      </div>\n";
},"14":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return ((stack1 = lookupProperty(helpers,"if").call(depth0 != null ? depth0 : (container.nullContext || {}),(depth0 != null ? lookupProperty(depth0,"department") : depth0),{"name":"if","hash":{},"fn":container.program(15, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":52,"column":6},"end":{"line":58,"column":6}}})) != null ? stack1 : "");
},"15":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "      <div class=\"ds-result__attribute\">\n        <span class=\"tag tag--sm ds-result__department\">\n          <span class=\"tag__label\">"
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"department") || (depth0 != null ? lookupProperty(depth0,"department") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"department","hash":{},"data":data,"loc":{"start":{"line":55,"column":35},"end":{"line":55,"column":49}}}) : helper)))
    + "</span>\n        </span>\n      </div>\n      ";
},"17":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "        <a class=\"ds-result__social\" href=\""
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"linkedin") || (depth0 != null ? lookupProperty(depth0,"linkedin") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"linkedin","hash":{},"data":data,"loc":{"start":{"line":61,"column":43},"end":{"line":61,"column":55}}}) : helper)))
    + "\" rel=\"noopener noreferrer nofollow\" target=\"_blank\" aria-label=\"LinkedIn Profile\">\n          <span class=\"fab fa-linkedin\" aria-hidden=\"true\"></span>\n        </a>\n";
},"19":function(container,depth0,helpers,partials,data) {
    var helper, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "        <a class=\"ds-result__social\" href=\"https://www.twitter.com/"
    + container.escapeExpression(((helper = (helper = lookupProperty(helpers,"twitter") || (depth0 != null ? lookupProperty(depth0,"twitter") : depth0)) != null ? helper : container.hooks.helperMissing),(typeof helper === "function" ? helper.call(depth0 != null ? depth0 : (container.nullContext || {}),{"name":"twitter","hash":{},"data":data,"loc":{"start":{"line":66,"column":67},"end":{"line":66,"column":78}}}) : helper)))
    + "\" rel=\"noopener noreferrer nofollow\" target=\"_blank\" aria-label=\"Twitter Profile\">\n          <span class=\"fab fa-twitter\" aria-hidden=\"true\"></span>\n        </a>\n";
},"21":function(container,depth0,helpers,partials,data,blockParams,depths) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "  <div class=\"ds-result__sources\">\n    <ul class=\"ds-sources-list\">\n"
    + ((stack1 = lookupProperty(helpers,"each").call(depth0 != null ? depth0 : (container.nullContext || {}),(depth0 != null ? lookupProperty(depth0,"sources") : depth0),{"name":"each","hash":{},"fn":container.program(22, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":84,"column":6},"end":{"line":98,"column":15}}})) != null ? stack1 : "")
    + "    </ul>\n  </div>\n";
},"22":function(container,depth0,helpers,partials,data,blockParams,depths) {
    var stack1, helper, options, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.lambda, alias3=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    }, buffer = 
  "      <li class=\"ds-sources-list__item "
    + ((stack1 = lookupProperty(helpers,"unless").call(alias1,(depth0 != null ? lookupProperty(depth0,"still_on_page") : depth0),{"name":"unless","hash":{},"fn":container.program(23, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":85,"column":39},"end":{"line":85,"column":111}}})) != null ? stack1 : "")
    + "\">\n"
    + ((stack1 = lookupProperty(helpers,"unless").call(alias1,(depth0 != null ? lookupProperty(depth0,"still_on_page") : depth0),{"name":"unless","hash":{},"fn":container.program(25, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":86,"column":8},"end":{"line":92,"column":19}}})) != null ? stack1 : "")
    + "        <a class=\"ds-sources-list__link\" href=\""
    + alias3(alias2((depth0 != null ? lookupProperty(depth0,"uri") : depth0), depth0))
    + "#hunter-email:"
    + alias3(alias2((depths[1] != null ? lookupProperty(depths[1],"value") : depths[1]), depth0))
    + "\" rel=\"noopener noreferrer nofollow\" target=\"_blank\">\n          "
    + alias3(alias2((depth0 != null ? lookupProperty(depth0,"uri") : depth0), depth0))
    + "\n        </a>\n        <time class=\"ds-sources-list__date\">";
  stack1 = ((helper = (helper = lookupProperty(helpers,"userDate") || (depth0 != null ? lookupProperty(depth0,"userDate") : depth0)) != null ? helper : container.hooks.helperMissing),(options={"name":"userDate","hash":{},"fn":container.program(27, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":96,"column":44},"end":{"line":96,"column":91}}}),(typeof helper === "function" ? helper.call(alias1,options) : helper));
  if (!lookupProperty(helpers,"userDate")) { stack1 = container.hooks.blockHelperMissing.call(depth0,stack1,options)}
  if (stack1 != null) { buffer += stack1; }
  return buffer + "</time>\n      </li>\n";
},"23":function(container,depth0,helpers,partials,data) {
    return "ds-sources-list__item--outdated";
},"25":function(container,depth0,helpers,partials,data) {
    return "        <span class=\"tag tag--sm\">\n          <span class=\"tag__label\" data-locale=\"removed\">\n          Removed\n          </span>\n        </span>\n";
},"27":function(container,depth0,helpers,partials,data) {
    var lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return container.escapeExpression(container.lambda((depth0 != null ? lookupProperty(depth0,"extracted_on") : depth0), depth0));
},"compiler":[8,">= 4.3.0"],"main":function(container,depth0,helpers,partials,data,blockParams,depths) {
    var stack1, helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "<div class=\"ds-result\">\n  <div class=\"ds-result__data\">\n    <div class=\"ds-result__primary\">\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"full_name") : depth0),{"name":"if","hash":{},"fn":container.program(1, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":4,"column":6},"end":{"line":6,"column":13}}})) != null ? stack1 : "")
    + "      <div class=\"ds-result__email copy-email\" data-email=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"value") || (depth0 != null ? lookupProperty(depth0,"value") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"value","hash":{},"data":data,"loc":{"start":{"line":7,"column":59},"end":{"line":7,"column":68}}}) : helper)))
    + "\" data-toggle=\"tooltip\" data-placement=\"top\" title=\"Click to copy\" data-locale-title=\"click_to_copy\">"
    + alias4(((helper = (helper = lookupProperty(helpers,"value") || (depth0 != null ? lookupProperty(depth0,"value") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"value","hash":{},"data":data,"loc":{"start":{"line":7,"column":169},"end":{"line":7,"column":178}}}) : helper)))
    + "</div>\n      <div class=\"ds-result__verification\">\n"
    + ((stack1 = (lookupProperty(helpers,"ifIsVerified")||(depth0 && lookupProperty(depth0,"ifIsVerified"))||alias2).call(alias1,((stack1 = (depth0 != null ? lookupProperty(depth0,"verification") : depth0)) != null ? lookupProperty(stack1,"status") : stack1),{"name":"ifIsVerified","hash":{},"fn":container.program(3, data, 0, blockParams, depths),"inverse":container.program(5, data, 0, blockParams, depths),"data":data,"loc":{"start":{"line":9,"column":8},"end":{"line":37,"column":25}}})) != null ? stack1 : "")
    + "      </div>\n    </div>\n    <div class=\"ds-result__secondary\">\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"phone_number") : depth0),{"name":"if","hash":{},"fn":container.program(10, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":41,"column":6},"end":{"line":46,"column":13}}})) != null ? stack1 : "")
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"position") : depth0),{"name":"if","hash":{},"fn":container.program(12, data, 0, blockParams, depths),"inverse":container.program(14, data, 0, blockParams, depths),"data":data,"loc":{"start":{"line":47,"column":6},"end":{"line":58,"column":13}}})) != null ? stack1 : "")
    + "      <div class=\"ds-result__attribute\">\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"linkedin") : depth0),{"name":"if","hash":{},"fn":container.program(17, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":60,"column":8},"end":{"line":64,"column":15}}})) != null ? stack1 : "")
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"twitter") : depth0),{"name":"if","hash":{},"fn":container.program(19, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":65,"column":8},"end":{"line":69,"column":15}}})) != null ? stack1 : "")
    + "      </div>\n    </div>\n    <div class=\"ds-result__save\">\n      "
    + ((stack1 = ((helper = (helper = lookupProperty(helpers,"lead_button") || (depth0 != null ? lookupProperty(depth0,"lead_button") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"lead_button","hash":{},"data":data,"loc":{"start":{"line":73,"column":6},"end":{"line":73,"column":23}}}) : helper))) != null ? stack1 : "")
    + "\n    </div>\n    <button aria-label=\"Toggle sources\" class=\"ds-result__source sources-link\" type=\"button\">\n      "
    + alias4(((helper = (helper = lookupProperty(helpers,"sources_link") || (depth0 != null ? lookupProperty(depth0,"sources_link") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"sources_link","hash":{},"data":data,"loc":{"start":{"line":76,"column":6},"end":{"line":76,"column":22}}}) : helper)))
    + "\n      <span class=\"far fa-angle-down\" aria-hidden=\"true\"></span>\n    </button>\n  </div>\n\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"sources") : depth0),{"name":"if","hash":{},"fn":container.program(21, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":81,"column":2},"end":{"line":101,"column":9}}})) != null ? stack1 : "")
    + "</div>\n";
},"useData":true,"useDepths":true});