import crafttweaker.item.IItemStack;

var Ritems = [
    <aroma1997sdimension:dimensionchanger>,
] as IItemStack[];
//
for item in Ritems{
    recipes.remove(item);
    item.addTooltip(format.darkRed("[SP] Banned Item"));
}