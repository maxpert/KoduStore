using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace KoduStore
{
    internal sealed class IndexInfo
    {        
        private readonly IList<MemberInfo> _memberFields;
        
        private IDictionary<MemberInfo, IList<IndexedAttribute>> _indexAttrubteMap;

        private IList<Tuple<MemberInfo, IndexedAttribute>> _indexMemberList;

        public Type Type { get; private set; }
        
        public IDictionary<MemberInfo, IList<IndexedAttribute>> IndexAttributeMap => _indexAttrubteMap;
        
        public IndexInfo(Type t)
        {
            this.Type = t;
            _memberFields = this.Type
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Cast<MemberInfo>()
                .Union(this.Type.GetFields(BindingFlags.Instance | BindingFlags.Public))
                .Where(p => p.GetCustomAttribute<IndexedAttribute>() != null)
                .ToList();

            this.InitializeIndexInfo();
        }

        public IList<Tuple<MemberInfo, IndexedAttribute>> GetIndexMembersList()
        {
            if (_indexMemberList != null)
            {
                return _indexMemberList;
            }

            var attributesList = new List<Tuple<MemberInfo, IndexedAttribute>>();
            foreach (var entryAttributes in _indexAttrubteMap)
            {
                foreach (var attribute in entryAttributes.Value)
                {
                    attributesList.Add(new Tuple<MemberInfo, IndexedAttribute>(entryAttributes.Key, attribute));
                }
            }

            _indexMemberList = attributesList;
            return attributesList;
        }
        
        private void InitializeIndexInfo()
        {
            _indexAttrubteMap = new Dictionary<MemberInfo, IList<IndexedAttribute>>();
            _indexMemberList = null;

            foreach(var member in _memberFields)
            {
                Type type;
                if (member is FieldInfo)
                {
                    type = ((FieldInfo)member).FieldType;
                }
                else
                {
                    type = ((PropertyInfo)member).PropertyType;
                }

                var attibutesList = member.GetCustomAttributes<IndexedAttribute>()
                    .Select(attribute =>
                    {
                        if (string.IsNullOrEmpty(attribute.Name))
                        {
                            attribute.Name = this.Type.FullName + "." + member.Name;
                        }

                        attribute.FieldType = attribute.FieldType ?? type;
                        return attribute;
                    });

                _indexAttrubteMap[member] = attibutesList.ToList();
            }
        }
    }
}
